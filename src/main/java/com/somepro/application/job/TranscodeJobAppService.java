package com.somepro.application.job;

import com.somepro.common.exception.BizException;
import com.somepro.domain.job.model.JobAttempt;
import com.somepro.domain.job.model.JobStatus;
import com.somepro.domain.job.model.TranscodeJob;
import com.somepro.domain.job.repository.TranscodeJobRepository;
import com.somepro.domain.media.model.AssetStatus;
import com.somepro.domain.media.model.MediaAsset;
import com.somepro.domain.media.repository.MediaAssetRepository;
import com.somepro.domain.profile.model.ProfileStatus;
import com.somepro.domain.profile.model.TranscodeProfile;
import com.somepro.domain.profile.repository.TranscodeProfileRepository;
import com.somepro.domain.shared.model.PageResult;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 转码任务用例编排（应用层）：提交 / 撤销 / 重试 / 节点领取 / 上报进度 / 上报结果 / 查看 / 分页。
 *
 * 不写业务规则（规则在领域层 TranscodeJob），只做编排：
 * - 提交前校验素材与档位状态、归属部门一致性、类型匹配、无同档位未完成任务；
 * - 重试前校验任务仍是失败态且未到尝试上限（领域层）、素材与档位还在且启用；
 * - 并发重复提交与任务编号分配由仓储在事务里兜底（见 TranscodeJobRepository.submitNew）；
 * - 领取 / 进度 / 结果 / 重试的并发互斥由仓储的条件更新兜底（见 TranscodeJobRepository 各 *If* 方法）；
 * - 出入参都是领域对象，不认识 PO、也不认识 VO。
 */
@Service
public class TranscodeJobAppService {

    private static final String DUPLICATE_MSG = "该素材在此档位下已有未完成的转码任务（待处理或处理中），请勿重复提交";

    private final TranscodeJobRepository transcodeJobRepository;
    private final MediaAssetRepository mediaAssetRepository;
    private final TranscodeProfileRepository transcodeProfileRepository;

    public TranscodeJobAppService(TranscodeJobRepository transcodeJobRepository,
                                  MediaAssetRepository mediaAssetRepository,
                                  TranscodeProfileRepository transcodeProfileRepository) {
        this.transcodeJobRepository = transcodeJobRepository;
        this.mediaAssetRepository = mediaAssetRepository;
        this.transcodeProfileRepository = transcodeProfileRepository;
    }

    /**
     * 提交转码任务：一个素材按一个档位转一次，提出来是 PENDING。
     *
     * 校验顺序：素材存在且可转码 → 归属部门与素材一致 → 档位存在且启用 →
     * 档位适用类型与素材类型匹配 → 同素材同档位无未完成任务。
     */
    public Mono<TranscodeJob> submit(Long assetId, Long profileId, String ownerDept, Integer priority) {
        return mediaAssetRepository.findById(assetId)
                .switchIfEmpty(Mono.error(new BizException("素材不存在：" + assetId)))
                .flatMap(asset -> {
                    requireAssetSubmittable(asset, ownerDept);
                    return transcodeProfileRepository.findById(profileId)
                            .switchIfEmpty(Mono.error(new BizException("转码档位不存在：" + profileId)))
                            .flatMap(profile -> {
                                requireProfileUsable(profile, asset);
                                return transcodeJobRepository.existsActiveByAssetAndProfile(assetId, profileId)
                                        .flatMap(exists -> {
                                            if (exists) {
                                                return Mono.<TranscodeJob>error(new BizException(DUPLICATE_MSG));
                                            }
                                            return transcodeJobRepository.submitNew(
                                                    TranscodeJob.submit(assetId, profileId, ownerDept, priority));
                                        });
                            });
                });
    }

    /** 撤销任务：只有 PENDING 可撤，且必须写明原因（规则在领域层，落库由仓储带条件兜底）。 */
    public Mono<TranscodeJob> cancel(Long id, String reason) {
        return transcodeJobRepository.findById(id)
                .switchIfEmpty(Mono.error(new BizException("转码任务不存在：" + id)))
                .flatMap(job -> {
                    job.cancel(reason);
                    return transcodeJobRepository.cancelIfPending(job);
                });
    }

    /**
     * 重试（重排）失败任务：FAILED → PENDING，重新排回队列等节点再领；素材跟着退回可转码。
     *
     * 校验顺序：任务存在且仍是失败态、未到建任务时定死的尝试上限（领域层 retry）→
     * 素材还在且未被停用 → 档位还在且未被停用。任一不过都给明确提示、不放进队列。
     * 重试只清失败说明 / 进度 / 起止时刻，attemptCount 保留，不新增执行记录；
     * 连点 / 多人同时点由仓储的条件更新兜底，只排一次（见 requeueIfFailed）。
     */
    public Mono<TranscodeJob> retry(Long id) {
        return transcodeJobRepository.findById(id)
                .switchIfEmpty(Mono.error(new BizException("转码任务不存在：" + id)))
                .flatMap(job -> {
                    // 先过领域规则：非 FAILED、或已到尝试上限，直接挡回，不查素材档位、不落库
                    job.retry();
                    return mediaAssetRepository.findById(job.getAssetId())
                            .switchIfEmpty(Mono.error(new BizException(
                                    "素材已删除，不能重试，请先恢复或换好素材：" + job.getAssetId())))
                            .flatMap(asset -> {
                                requireAssetRetriable(asset);
                                return transcodeProfileRepository.findById(job.getProfileId())
                                        .switchIfEmpty(Mono.error(new BizException(
                                                "转码档位已删除，不能重试，请先恢复或换好档位：" + job.getProfileId())))
                                        .flatMap(profile -> {
                                            requireProfileRetriable(profile);
                                            return transcodeJobRepository.requeueIfFailed(job);
                                        });
                            });
                });
    }

    /**
     * 节点领取任务：PENDING → RUNNING，素材跟着进转码中，同时记一条执行记录。
     * 同一任务同一时刻只放一台节点；已被领走的、已出结果的再来领，都给明确提示挡回去。
     */
    public Mono<TranscodeJob> claim(Long id, String workerCode) {
        return transcodeJobRepository.findById(id)
                .switchIfEmpty(Mono.error(new BizException("转码任务不存在：" + id)))
                .flatMap(job -> {
                    job.claim();
                    // 执行记录与任务状态同事务落库：第几次跑 = 领取后的 attemptCount
                    JobAttempt attempt = JobAttempt.start(job.getId(), job.getAttemptCount(),
                            workerCode, job.getStartedAt());
                    return transcodeJobRepository.claimIfPending(job, attempt);
                });
    }

    /** 节点上报进度：0-100 的整数、只能往前；没被领走的任务不该收到进度。 */
    public Mono<TranscodeJob> reportProgress(Long id, Integer progress) {
        return transcodeJobRepository.findById(id)
                .switchIfEmpty(Mono.error(new BizException("转码任务不存在：" + id)))
                .flatMap(job -> {
                    job.reportProgress(progress);
                    return transcodeJobRepository.reportProgressIfRunning(job);
                });
    }

    /**
     * 节点上报结果：SUCCESS 或 FAILED，可带产出路径 / 失败原因与结束时刻（缺省取当前时刻）。
     * 成功素材留在转码中等人审，失败素材退回可转码；已出结果的任务不会再被改第二遍。
     */
    public Mono<TranscodeJob> reportResult(Long id, String result, String outputPath,
                                           String errorMsg, LocalDateTime finishedAt) {
        return transcodeJobRepository.findById(id)
                .switchIfEmpty(Mono.error(new BizException("转码任务不存在：" + id)))
                .flatMap(job -> {
                    if (isSuccess(result)) {
                        job.succeed(outputPath, finishedAt);
                    } else {
                        job.fail(errorMsg, finishedAt);
                    }
                    return transcodeJobRepository.finishIfRunning(job);
                });
    }

    /** 某任务的执行记录：第几次跑、哪台节点领的、几点开始、几点结束。 */
    public Mono<List<JobAttempt>> listAttempts(Long id) {
        return transcodeJobRepository.findById(id)
                .switchIfEmpty(Mono.error(new BizException("转码任务不存在：" + id)))
                .flatMap(job -> transcodeJobRepository.listAttempts(id));
    }

    /** 结果参数只认 SUCCESS / FAILED（大小写不敏感），其余值直接挡回。 */
    private static boolean isSuccess(String result) {
        if (result == null || result.isBlank()) {
            throw new BizException("结果不能为空（SUCCESS/FAILED）");
        }
        String normalized = result.trim().toUpperCase();
        if (!JobStatus.SUCCESS.name().equals(normalized) && !JobStatus.FAILED.name().equals(normalized)) {
            throw new BizException("结果只支持 SUCCESS/FAILED：" + result);
        }
        return JobStatus.SUCCESS.name().equals(normalized);
    }

    public Mono<TranscodeJob> get(Long id) {
        return transcodeJobRepository.findById(id)
                .switchIfEmpty(Mono.error(new BizException("转码任务不存在：" + id)));
    }

    /** 分页翻查：条件都可空，一个都不填就是全量分页。 */
    public Mono<PageResult<TranscodeJob>> page(int pageNum, int pageSize, String jobNo, String status,
                                               String ownerDept, Long assetId, Long profileId) {
        return transcodeJobRepository.page(pageNum, pageSize, jobNo, normalize(status),
                ownerDept, assetId, profileId);
    }

    /** 素材可提交校验：DONE（已完成）与 DISABLED（已停用）不能再提；归属部门必须与素材一致。 */
    private static void requireAssetSubmittable(MediaAsset asset, String ownerDept) {
        if (asset.getStatus() == AssetStatus.DONE) {
            throw new BizException("素材已完成转码，无需再提交：" + asset.getId());
        }
        if (asset.getStatus() == AssetStatus.DISABLED) {
            throw new BizException("素材已停用，不能提交转码：" + asset.getId());
        }
        if (ownerDept == null || ownerDept.isBlank()) {
            throw new BizException("归属部门不能为空");
        }
        if (!asset.getOwnerDept().equals(ownerDept.trim())) {
            throw new BizException("归属部门与素材不一致，素材归属部门：" + asset.getOwnerDept());
        }
    }

    /** 档位可用校验：必须启用，且适用媒体类型与素材一致（图片素材天然匹配不到任何档位）。 */
    private static void requireProfileUsable(TranscodeProfile profile, MediaAsset asset) {
        if (profile.getStatus() != ProfileStatus.ENABLED) {
            throw new BizException("转码档位已停用：" + profile.getId());
        }
        if (!profile.getMediaType().name().equals(asset.getMediaType().name())) {
            throw new BizException("档位适用类型（" + profile.getMediaType()
                    + "）与素材类型（" + asset.getMediaType() + "）不匹配");
        }
    }

    /** 重试前素材校验：被停用的素材不能重排（删除的在查不到那一步已挡回），得先把料拾掇好。 */
    private static void requireAssetRetriable(MediaAsset asset) {
        if (asset.getStatus() == AssetStatus.DISABLED) {
            throw new BizException("素材已停用，不能重试，请先启用素材：" + asset.getId());
        }
    }

    /** 重试前档位校验：被停用的档位不能重排（删除的在查不到那一步已挡回），得先把规格拾掇好。 */
    private static void requireProfileRetriable(TranscodeProfile profile) {
        if (profile.getStatus() != ProfileStatus.ENABLED) {
            throw new BizException("转码档位已停用，不能重试，请先启用档位：" + profile.getId());
        }
    }

    /** 枚举类查询条件统一转大写，调用方传小写也能查到。 */
    private static String normalize(String value) {
        return (value == null || value.isBlank()) ? null : value.trim().toUpperCase();
    }
}
