package com.somepro.infrastructure.persistence.job;

import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.github.pagehelper.PageHelper;
import com.somepro.common.exception.BizException;
import com.somepro.domain.job.model.AttemptStatus;
import com.somepro.domain.job.model.JobAttempt;
import com.somepro.domain.job.model.JobStatus;
import com.somepro.domain.job.model.TranscodeJob;
import com.somepro.domain.job.repository.TranscodeJobRepository;
import com.somepro.domain.media.model.AssetStatus;
import com.somepro.domain.profile.model.ProfileStatus;
import com.somepro.domain.shared.model.PageResult;
import com.somepro.infrastructure.config.ReactiveOperatorContext;
import com.somepro.infrastructure.persistence.audit.AuditContextHolder;
import com.somepro.infrastructure.persistence.job.converter.JobAttemptPoConverter;
import com.somepro.infrastructure.persistence.job.converter.TranscodeJobPoConverter;
import com.somepro.infrastructure.persistence.job.po.JobAttemptPO;
import com.somepro.infrastructure.persistence.job.po.TranscodeJobPO;
import com.somepro.infrastructure.persistence.media.MediaAssetMapper;
import com.somepro.infrastructure.persistence.media.po.MediaAssetPO;
import com.somepro.infrastructure.persistence.profile.TranscodeProfileMapper;
import com.somepro.infrastructure.persistence.profile.po.TranscodeProfilePO;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.LocalDate;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * 转码任务仓储适配器：用 MyBatis-Plus 实现领域仓储端口（基础设施层）。
 *
 * 约定同 DemoItemRepositoryImpl：
 * - 所有 DB 调用必须经 {@link #blocking} 桥接到 boundedElastic，严禁在 event-loop 上跑 JDBC；
 * - Mapper 只认 {@link TranscodeJobPO}，领域层只认 {@link TranscodeJob}，两者在本类里互转；
 * - 软删除交给 @TableLogic，不手写 del_flag 条件；
 * - 分页统一用 PageHelper.startPage()，finally 里必须 clearPage()。
 *
 * 本模块特有的并发防线：
 * 1. 提交时事务内 SELECT ... FOR UPDATE 锁住素材行，锁内复查「同素材同档位无未完成任务」，
 *    同一素材的并发提交因此串行，不会重复落库；
 * 2. 任务编号 uk_job_no 唯一索引兜底，撞号（不同素材并发取到同一序号）时重取编号重试；
 * 3. 领取 / 进度 / 结果 / 重试都走「乐观条件更新」（UPDATE ... WHERE status=期望值）：
 *    InnoDB 行锁把并发请求串行，UPDATE 按当前读评估 WHERE，同一时刻只有一个请求能改成功，
 *    其余 rows=0，重读当前状态后给明确提示 —— 几个节点一起抢也只放一台；
 *    任务到了终态后，后续上报全部 rows=0，不会被重新改一遍，执行记录也不会再多出来；
 *    失败重试同理（WHERE status=FAILED），一条任务被连点/多人同时点也只重排一次。
 */
@Repository
public class TranscodeJobRepositoryImpl implements TranscodeJobRepository {

    /** 任务编号撞号时的最大重试次数。 */
    private static final int JOB_NO_MAX_RETRY = 5;

    private static final String DUPLICATE_MSG = "该素材在此档位下已有未完成的转码任务（待处理或处理中），请勿重复提交";

    private final TranscodeJobMapper transcodeJobMapper;
    private final MediaAssetMapper mediaAssetMapper;
    private final TranscodeProfileMapper transcodeProfileMapper;
    private final JobAttemptMapper jobAttemptMapper;
    private final TransactionTemplate transactionTemplate;

    public TranscodeJobRepositoryImpl(TranscodeJobMapper transcodeJobMapper,
                                      MediaAssetMapper mediaAssetMapper,
                                      TranscodeProfileMapper transcodeProfileMapper,
                                      JobAttemptMapper jobAttemptMapper,
                                      PlatformTransactionManager transactionManager) {
        this.transcodeJobMapper = transcodeJobMapper;
        this.mediaAssetMapper = mediaAssetMapper;
        this.transcodeProfileMapper = transcodeProfileMapper;
        this.jobAttemptMapper = jobAttemptMapper;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @Override
    public Mono<TranscodeJob> submitNew(TranscodeJob job) {
        return blocking(() -> transactionTemplate.execute(status -> {
            // ① 锁住素材行（FOR UPDATE）：同一素材的并发提交在此串行，
            //    配合 ② 的锁内复查，挡住「同素材同档位」的并发重复提交
            MediaAssetPO asset = mediaAssetMapper.selectOne(Wrappers.<MediaAssetPO>lambdaQuery()
                    .eq(MediaAssetPO::getId, job.getAssetId())
                    .last("FOR UPDATE"));
            if (asset == null) {
                throw new BizException("素材不存在：" + job.getAssetId());
            }
            // ② 锁内复查：同素材同档位是否已有未完成任务（待处理/处理中）
            if (countActive(job.getAssetId(), job.getProfileId()) > 0) {
                throw new BizException(DUPLICATE_MSG);
            }
            // ③ 分配任务编号并落库；uk_job_no 兜底，撞号时重取编号重试
            TranscodeJobPO po = TranscodeJobPoConverter.toPo(job);
            po.setId(IdUtil.getSnowflakeNextId());
            insertWithFreshJobNo(po);
            // insert 后框架会回填审计字段，转回领域对象一并返回
            return TranscodeJobPoConverter.toDomain(po);
        }));
    }

    @Override
    public Mono<TranscodeJob> findById(Long id) {
        return blocking(() -> {
            TranscodeJobPO po = transcodeJobMapper.selectById(id);
            // 返回 null 时 Mono.fromCallable 会自动转成空信号
            return po == null ? null : TranscodeJobPoConverter.toDomain(po);
        });
    }

    @Override
    public Mono<Boolean> existsActiveByAssetAndProfile(Long assetId, Long profileId) {
        return blocking(() -> countActive(assetId, profileId) > 0);
    }

    @Override
    public Mono<PageResult<TranscodeJob>> page(int pageNum, int pageSize, String jobNo, String status,
                                               String ownerDept, Long assetId, Long profileId) {
        return this.<PageResult<TranscodeJob>>blocking(() -> {
            try {
                PageHelper.startPage(pageNum, pageSize);
                // 条件全部可空：一个都不填时不拼任何条件，全量分页（早先录的数据也能翻出来）
                LambdaQueryWrapper<TranscodeJobPO> wrapper = Wrappers.<TranscodeJobPO>lambdaQuery()
                        .eq(StrUtil.isNotBlank(jobNo), TranscodeJobPO::getJobNo, jobNo)
                        .eq(StrUtil.isNotBlank(status), TranscodeJobPO::getStatus, status)
                        .eq(StrUtil.isNotBlank(ownerDept), TranscodeJobPO::getOwnerDept, ownerDept)
                        .eq(assetId != null, TranscodeJobPO::getAssetId, assetId)
                        .eq(profileId != null, TranscodeJobPO::getProfileId, profileId)
                        .orderByDesc(TranscodeJobPO::getId);
                List<TranscodeJobPO> rows = transcodeJobMapper.selectList(wrapper);
                // 命中分页插件时返回的是 com.github.pagehelper.Page，可直接取总数
                long total = rows instanceof com.github.pagehelper.Page
                        ? ((com.github.pagehelper.Page<?>) rows).getTotal()
                        : rows.size();
                List<TranscodeJob> content = rows.stream()
                        .map(TranscodeJobPoConverter::toDomain)
                        .collect(Collectors.toList());
                return new PageResult<>(content, total, pageNum, pageSize);
            } finally {
                // 分页插件靠 ThreadLocal 传递分页参数，必须清理，否则污染线程池里的下一次调用
                PageHelper.clearPage();
            }
        });
    }

    @Override
    public Mono<TranscodeJob> cancelIfPending(TranscodeJob job) {
        return blocking(() -> {
            TranscodeJobPO update = new TranscodeJobPO();
            update.setStatus(job.getStatus().name());
            update.setErrorMsg(job.getErrorMsg());
            update.setFinishedAt(job.getFinishedAt());
            // 乐观条件更新：只有库里仍是 PENDING 的行才会被改掉，
            // 防止「查出来是待处理 → 节点同时领走 → 又被撤销」的并发窗口。
            // update(entity, wrapper) 会走 MetaObjectHandler 填充 updateBy/updateTime。
            int rows = transcodeJobMapper.update(update, Wrappers.<TranscodeJobPO>lambdaUpdate()
                    .eq(TranscodeJobPO::getId, job.getId())
                    .eq(TranscodeJobPO::getStatus, JobStatus.PENDING.name()));
            if (rows == 0) {
                throw new BizException("任务已被节点领取或已结束，无法撤销：" + job.getJobNo());
            }
            return TranscodeJobPoConverter.toDomain(transcodeJobMapper.selectById(job.getId()));
        });
    }

    @Override
    public Mono<TranscodeJob> claimIfPending(TranscodeJob job, JobAttempt attempt) {
        return blocking(() -> transactionTemplate.execute(status -> {
            // ① 原子领取：仅当库里仍是 PENDING 才改成 RUNNING。
            //    几个节点同时抢时 InnoDB 行锁把它们串行，后到的 UPDATE 按当前读重评 WHERE，
            //    看到状态已变就 rows=0 —— 同一时刻只放一台节点领到。
            TranscodeJobPO update = new TranscodeJobPO();
            update.setStatus(job.getStatus().name());
            update.setStartedAt(job.getStartedAt());
            update.setProgress(job.getProgress());
            update.setAttemptCount(job.getAttemptCount());
            int rows = transcodeJobMapper.update(update, Wrappers.<TranscodeJobPO>lambdaUpdate()
                    .eq(TranscodeJobPO::getId, job.getId())
                    .eq(TranscodeJobPO::getStatus, JobStatus.PENDING.name()));
            if (rows == 0) {
                throw new BizException(conflictMessage(job.getId(), "领取"));
            }
            // ② 对应素材跟着进转码中
            mediaAssetMapper.update(new MediaAssetPO(), Wrappers.<MediaAssetPO>lambdaUpdate()
                    .set(MediaAssetPO::getStatus, AssetStatus.TRANSCODING.name())
                    .eq(MediaAssetPO::getId, job.getAssetId()));
            // ③ 记一条执行记录：第几次跑、哪台节点领的、几点开始
            JobAttemptPO attemptPo = JobAttemptPoConverter.toPo(attempt);
            attemptPo.setId(IdUtil.getSnowflakeNextId());
            jobAttemptMapper.insert(attemptPo);
            return TranscodeJobPoConverter.toDomain(transcodeJobMapper.selectById(job.getId()));
        }));
    }

    @Override
    public Mono<TranscodeJob> reportProgressIfRunning(TranscodeJob job) {
        return blocking(() -> {
            // 乐观条件更新：库里仍是 RUNNING 且库里进度不超过本次上报值才生效。
            // 没被领走的（PENDING）与已出结果的（终态）rows=0，进度报不进来；
            // 并发下比当前小的上报也 rows=0，进度不会回退。
            TranscodeJobPO update = new TranscodeJobPO();
            update.setProgress(job.getProgress());
            int rows = transcodeJobMapper.update(update, Wrappers.<TranscodeJobPO>lambdaUpdate()
                    .eq(TranscodeJobPO::getId, job.getId())
                    .eq(TranscodeJobPO::getStatus, JobStatus.RUNNING.name())
                    .le(TranscodeJobPO::getProgress, job.getProgress()));
            if (rows == 0) {
                throw new BizException(progressConflictMessage(job.getId(), job.getProgress()));
            }
            return TranscodeJobPoConverter.toDomain(transcodeJobMapper.selectById(job.getId()));
        });
    }

    @Override
    public Mono<TranscodeJob> finishIfRunning(TranscodeJob job) {
        return blocking(() -> transactionTemplate.execute(status -> {
            // ① 原子收尾：仅当库里仍是 RUNNING 才改成终态。
            //    已出结果的任务 rows=0，后面再怎么报都不会把它重新改一遍。
            TranscodeJobPO update = new TranscodeJobPO();
            update.setStatus(job.getStatus().name());
            update.setProgress(job.getProgress());
            update.setOutputPath(job.getOutputPath());
            update.setErrorMsg(job.getErrorMsg());
            update.setFinishedAt(job.getFinishedAt());
            int rows = transcodeJobMapper.update(update, Wrappers.<TranscodeJobPO>lambdaUpdate()
                    .eq(TranscodeJobPO::getId, job.getId())
                    .eq(TranscodeJobPO::getStatus, JobStatus.RUNNING.name()));
            if (rows == 0) {
                throw new BizException(conflictMessage(job.getId(), "上报结果"));
            }
            // ② 素材联动：成功留在转码中等人审（不动）；失败退回可转码，回头还能再提
            if (job.getStatus() == JobStatus.FAILED) {
                mediaAssetMapper.update(new MediaAssetPO(), Wrappers.<MediaAssetPO>lambdaUpdate()
                        .set(MediaAssetPO::getStatus, AssetStatus.READY.name())
                        .eq(MediaAssetPO::getId, job.getAssetId()));
            }
            // ③ 当前这条执行记录跟着收尾（按 job_id + attempt_no 定位，只更新不新增；
            //    终态任务不会再被领取，执行记录也就不会再多出来）
            jobAttemptMapper.update(new JobAttemptPO(), Wrappers.<JobAttemptPO>lambdaUpdate()
                    .set(JobAttemptPO::getStatus, job.getStatus().name())
                    .set(JobAttemptPO::getErrorMsg, job.getErrorMsg())
                    .set(JobAttemptPO::getFinishedAt, job.getFinishedAt())
                    .eq(JobAttemptPO::getJobId, job.getId())
                    .eq(JobAttemptPO::getAttemptNo, job.getAttemptCount())
                    .eq(JobAttemptPO::getStatus, AttemptStatus.RUNNING.name()));
            return TranscodeJobPoConverter.toDomain(transcodeJobMapper.selectById(job.getId()));
        }));
    }

    @Override
    public Mono<TranscodeJob> requeueIfFailed(TranscodeJob job) {
        return blocking(() -> transactionTemplate.execute(status -> {
            // ① 先抢任务行：仅当库里仍是 FAILED 才改回 PENDING。
            //    同一条任务连点几下、几个人同时点时，InnoDB 行锁把更新串行，
            //    第一个请求改成功，后续请求再按当前读重评 WHERE 都 rows=0 —— 只排一次。
            //    先锁 job 再碰素材（与 claimIfPending / finishIfRunning 同序），
            //    避免「重试持素材锁等任务锁、领取持任务锁等素材锁」的锁顺序倒置死锁。
            //    error_msg / started_at / finished_at 要清成 NULL，必须走 wrapper.set(...)
            //    （update(entity, wrapper) 只搬实体里的非空字段，set 不进 NULL）。
            int rows = transcodeJobMapper.update(new TranscodeJobPO(),
                    Wrappers.<TranscodeJobPO>lambdaUpdate()
                            .set(TranscodeJobPO::getStatus, JobStatus.PENDING.name())
                            .set(TranscodeJobPO::getProgress, 0)
                            .set(TranscodeJobPO::getErrorMsg, null)
                            .set(TranscodeJobPO::getStartedAt, null)
                            .set(TranscodeJobPO::getFinishedAt, null)
                            .eq(TranscodeJobPO::getId, job.getId())
                            .eq(TranscodeJobPO::getStatus, JobStatus.FAILED.name()));
            if (rows == 0) {
                throw new BizException(requeueConflictMessage(job.getId()));
            }
            // ② 锁内复查素材与档位：停用 / 删除（@TableLogic 下查不到）的一律抛异常，
            //    事务回滚，任务退回 FAILED —— 得先把料和规格拾掇好再来。
            //    FOR UPDATE 挡住「复查通过 → 紧接着被停用/删除 → 照样入队」的并发窗口。
            MediaAssetPO asset = mediaAssetMapper.selectOne(Wrappers.<MediaAssetPO>lambdaQuery()
                    .eq(MediaAssetPO::getId, job.getAssetId())
                    .last("FOR UPDATE"));
            if (asset == null) {
                throw new BizException("素材已删除，不能重试，请先恢复或更换素材：" + job.getAssetId());
            }
            if (AssetStatus.DISABLED.name().equals(asset.getStatus())) {
                throw new BizException("素材已停用，不能重试，请先启用素材：" + job.getAssetId());
            }
            TranscodeProfilePO profile = transcodeProfileMapper.selectOne(
                    Wrappers.<TranscodeProfilePO>lambdaQuery()
                            .eq(TranscodeProfilePO::getId, job.getProfileId())
                            .last("FOR UPDATE"));
            if (profile == null) {
                throw new BizException("转码档位已删除，不能重试，请先恢复或更换档位：" + job.getProfileId());
            }
            if (ProfileStatus.DISABLED.name().equals(profile.getStatus())) {
                throw new BizException("转码档位已停用，不能重试，请先启用档位：" + job.getProfileId());
            }
            // ③ 素材跟着回到可领状态；等下次被节点领走再进转码中。
            //    本步不碰执行记录：下次领走时才接着上一次的 attemptNo 往下记。
            mediaAssetMapper.update(new MediaAssetPO(), Wrappers.<MediaAssetPO>lambdaUpdate()
                    .set(MediaAssetPO::getStatus, AssetStatus.READY.name())
                    .eq(MediaAssetPO::getId, job.getAssetId()));
            return TranscodeJobPoConverter.toDomain(transcodeJobMapper.selectById(job.getId()));
        }));
    }

    @Override
    public Mono<List<JobAttempt>> listAttempts(Long jobId) {
        return blocking(() -> jobAttemptMapper.selectList(Wrappers.<JobAttemptPO>lambdaQuery()
                        .eq(JobAttemptPO::getJobId, jobId)
                        .orderByAsc(JobAttemptPO::getAttemptNo))
                .stream()
                .map(JobAttemptPoConverter::toDomain)
                .collect(Collectors.toList()));
    }

    /** 条件更新 rows=0 时的明确提示：重读当前状态，告诉调用方任务现在到底什么样。 */
    private String conflictMessage(Long jobId, String action) {        TranscodeJobPO current = transcodeJobMapper.selectById(jobId);
        if (current == null) {
            return "转码任务不存在：" + jobId;
        }
        return action + "失败，任务当前状态：" + current.getStatus()
                + "（可能已被其他节点领取或已出结果）";
    }

    /** 重试 rows=0 时的明确提示：重读当前状态，告诉调用方任务现在到底什么样（已不是失败态）。 */
    private String requeueConflictMessage(Long jobId) {
        TranscodeJobPO current = transcodeJobMapper.selectById(jobId);
        if (current == null) {
            return "转码任务不存在：" + jobId;
        }
        return "重试失败，任务当前状态：" + current.getStatus()
                + "（可能已被其他人重试并重新排队，请勿重复操作）";
    }

    /** 进度 rows=0 时的明确提示：区分「状态不对」与「进度回退」两种原因。 */
    private String progressConflictMessage(Long jobId, Integer progress) {
        TranscodeJobPO current = transcodeJobMapper.selectById(jobId);
        if (current == null) {
            return "转码任务不存在：" + jobId;
        }
        if (!JobStatus.RUNNING.name().equals(current.getStatus())) {
            return "进度上报失败，任务当前状态：" + current.getStatus()
                    + "（未被领取或已出结果，不该收到进度）";
        }
        return "进度只能往前，不能回退：当前已 " + current.getProgress() + "%，上报 " + progress + "%";
    }

    /** 同素材同档位的未完成任务数（PENDING/RUNNING）。 */
    private long countActive(Long assetId, Long profileId) {
        Long count = transcodeJobMapper.selectCount(Wrappers.<TranscodeJobPO>lambdaQuery()
                .eq(TranscodeJobPO::getAssetId, assetId)
                .eq(TranscodeJobPO::getProfileId, profileId)
                .in(TranscodeJobPO::getStatus, JobStatus.PENDING.name(), JobStatus.RUNNING.name()));
        return count == null ? 0 : count;
    }

    /** 取号并插入；撞 uk_job_no（并发取到同一序号）时重取编号重试。 */
    private void insertWithFreshJobNo(TranscodeJobPO po) {
        for (int attempt = 1; ; attempt++) {
            po.setJobNo(nextJobNo());
            try {
                transcodeJobMapper.insert(po);
                return;
            } catch (DuplicateKeyException e) {
                if (attempt >= JOB_NO_MAX_RETRY) {
                    throw new BizException("任务编号生成冲突，请稍后重试");
                }
            }
        }
    }

    /**
     * 任务编号：TJ-年份-四位序号，按年递增（如 TJ-2026-0001）。
     *
     * 取当前最大编号 +1；按 LENGTH 再按字典序倒排，序号超过 4 位（10000+）时也不会取错最大值。
     * REGEXP 限定数字后缀，挡住历史脏数据干扰取号。
     */
    private String nextJobNo() {
        String prefix = "TJ-" + LocalDate.now().getYear() + "-";
        TranscodeJobPO latest = transcodeJobMapper.selectOne(Wrappers.<TranscodeJobPO>lambdaQuery()
                .likeRight(TranscodeJobPO::getJobNo, prefix)
                .apply("job_no REGEXP {0}", prefix + "[0-9]+")
                .last("ORDER BY LENGTH(job_no) DESC, job_no DESC LIMIT 1"));
        int next = 1;
        if (latest != null) {
            next = Integer.parseInt(latest.getJobNo().substring(prefix.length())) + 1;
        }
        return prefix + String.format("%04d", next);
    }

    /**
     * 阻塞 DB 调用 → 响应式链路的桥接器。
     *
     * 1. 先在响应式线程上从 Reactor Context 取操作人（切线程后就取不到了）
     * 2. 再切到 boundedElastic 执行 JDBC
     * 3. 把操作人放进 AuditContextHolder，供 MetaObjectHandler 填充 createBy / updateBy
     */
    private <T> Mono<T> blocking(Supplier<T> supplier) {
        return Mono.deferContextual(ctx -> {
            String operator = ReactiveOperatorContext.getOperator(ctx);
            return Mono.fromCallable(() -> {
                AuditContextHolder.setOperator(operator);
                try {
                    return supplier.get();
                } finally {
                    AuditContextHolder.clear();
                }
            }).subscribeOn(Schedulers.boundedElastic());
        });
    }
}
