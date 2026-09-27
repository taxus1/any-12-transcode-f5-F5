package com.somepro.domain.job.model;

import com.somepro.common.exception.BizException;
import com.somepro.domain.shared.model.BaseEntity;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

/**
 * 转码任务聚合根（job 上下文）：一个素材按一个档位转一次。
 *
 * 纯领域对象：只描述业务与不变量，不带任何持久化注解（表映射在基础设施层的 TranscodeJobPO）。
 *
 * 不变量：
 * - 必须指定素材 id、档位 id、归属部门；优先级越小越先做（1-99，缺省 5）；
 * - 新提交的任务一律 PENDING（待处理，等节点来领），attemptCount=0、progress=0、maxAttempts=3，
 *   submittedAt 记提交时刻；
 * - 只有 PENDING 能被节点领取（claim），领取后 RUNNING、记开始时刻、已尝试次数 +1；
 * - 只有 RUNNING 能报进度、出结果；进度是 0-100 的整数且只能往前；
 * - SUCCESS / CANCELLED 是终态：出了结果的任务不再接受任何上报，也不会再被领取；
 * - FAILED 在已尝试次数未到上限（maxAttempts，提交时定死）时，可由有权限的人重排（retry）
 *   回 PENDING 重新排队：清掉失败说明、进度归零、起止时刻清空，attemptCount 原样保留，
 *   不新增执行记录（下次被领取时 attemptNo 接着上一次往下排）；到了上限的 FAILED 不能再重排；
 * - 撤销只能发生在 PENDING（还没被节点领走），且必须写明撤销原因；
 *   表里没有单独的取消原因列，原因落在 errorMsg；
 * - 任务编号 jobNo 形如 TJ-2026-0001，由仓储按年顺序分配（应用层不给编号）。
 */
@Getter
@Setter
public class TranscodeJob extends BaseEntity {

    /** 缺省优先级（越小越先做）。 */
    public static final int DEFAULT_PRIORITY = 5;

    /** 缺省最多尝试次数。 */
    public static final int DEFAULT_MAX_ATTEMPTS = 3;

    private Long id;

    /** 任务编号，全局唯一（形如 TJ-2026-0001），由仓储在落库时分配。 */
    private String jobNo;

    /** 素材 id（t_media_asset.id）。 */
    private Long assetId;

    /** 档位 id（t_transcode_profile.id）。 */
    private Long profileId;

    /** 归属部门（冗余自素材，配额与台账按它归集）。 */
    private String ownerDept;

    /** 优先级，越小越先做。 */
    private Integer priority;

    private JobStatus status;

    /** 已尝试次数。 */
    private Integer attemptCount;

    /** 最多尝试次数。 */
    private Integer maxAttempts;

    /** 进度 0-100。 */
    private Integer progress;

    /** 产出文件路径。 */
    private String outputPath;

    /** 最近一次失败原因；撤销时也用它记录撤销原因（表里无单独的取消原因列）。 */
    private String errorMsg;

    /** 提交时刻。 */
    private LocalDateTime submittedAt;

    /** 开始处理时刻。 */
    private LocalDateTime startedAt;

    /** 结束时刻（成功/失败/取消）。 */
    private LocalDateTime finishedAt;

    /** 审核结果（PASS/REJECT），本聚合当前不推进，留给后续审核流程。 */
    private String reviewResult;
    private String reviewComment;
    private String reviewBy;
    private LocalDateTime reviewTime;

    /** 工厂方法：提交转码任务。新任务一律 PENDING，并保证初始不变量。 */
    public static TranscodeJob submit(Long assetId, Long profileId, String ownerDept, Integer priority) {
        TranscodeJob job = new TranscodeJob();
        job.setAssetId(assetId);
        job.setProfileId(profileId);
        job.setOwnerDept(ownerDept);
        job.setPriority(priority == null ? DEFAULT_PRIORITY : priority);
        job.setStatus(JobStatus.PENDING);
        job.setAttemptCount(0);
        job.setMaxAttempts(DEFAULT_MAX_ATTEMPTS);
        job.setProgress(0);
        job.setSubmittedAt(LocalDateTime.now());
        job.validate();
        return job;
    }

    /**
     * 领域行为：节点领取任务。
     *
     * - 只有 PENDING（还压在待处理里）的任务能领；已被领走的（RUNNING）、
     *   已出结果的（SUCCESS/FAILED）、已撤销的（CANCELLED）都挡回去；
     * - 领取后 RUNNING，startedAt 记开始时刻，progress 清零重新跑，attemptCount +1
     *   （第几次跑，与执行记录 JobAttempt.attemptNo 对齐）。
     *
     * 注意：这里只校验「当前看到的状态」；几个节点同时来抢时，
     * 由仓储的条件更新（WHERE status=PENDING）兜底，只放一台进去（见 claimIfPending）。
     */
    public void claim() {
        if (status != JobStatus.PENDING) {
            throw new BizException("只有待处理（PENDING）的任务才能被节点领取，当前状态：" + status);
        }
        this.status = JobStatus.RUNNING;
        this.startedAt = LocalDateTime.now();
        this.progress = 0;
        this.attemptCount = this.attemptCount + 1;
    }

    /**
     * 领域行为：节点上报进度。
     *
     * - 只有 RUNNING 能报：没被领走的（PENDING）不该收到进度，
     *   已出结果的（SUCCESS/FAILED）不许再改；
     * - 进度是 0-100 的整数，且只能往前：报得比当前小直接挡回去（报一样的不算回退）。
     */
    public void reportProgress(Integer progress) {
        if (progress == null) {
            throw new BizException("进度不能为空");
        }
        if (progress < 0 || progress > 100) {
            throw new BizException("进度必须是 0-100 的整数：" + progress);
        }
        if (status != JobStatus.RUNNING) {
            throw new BizException("只有处理中（RUNNING）的任务才能上报进度，当前状态：" + status);
        }
        if (progress < this.progress) {
            throw new BizException("进度只能往前，不能回退：当前已 " + this.progress + "%，上报 " + progress + "%");
        }
        this.progress = progress;
    }

    /**
     * 领域行为：节点上报成功。任务出终态 SUCCESS，进度顶到 100，记结束时刻（缺省取当前时刻）。
     * 素材留在转码中等人审（素材联动在仓储层一并落库）。
     */
    public void succeed(String outputPath, LocalDateTime finishedAt) {
        requireRunning();
        this.status = JobStatus.SUCCESS;
        this.progress = 100;
        if (outputPath != null && !outputPath.isBlank()) {
            this.outputPath = outputPath.trim();
        }
        this.finishedAt = finishedAt == null ? LocalDateTime.now() : finishedAt;
    }

    /**
     * 领域行为：节点上报失败。任务出终态 FAILED，记失败原因与结束时刻（缺省取当前时刻）。
     * 素材退回可转码（素材联动在仓储层一并落库），回头还能再提。
     */
    public void fail(String errorMsg, LocalDateTime finishedAt) {
        requireRunning();
        this.status = JobStatus.FAILED;
        if (errorMsg != null && !errorMsg.isBlank()) {
            this.errorMsg = errorMsg.trim();
        }
        this.finishedAt = finishedAt == null ? LocalDateTime.now() : finishedAt;
    }

    /** 出结果的前置守卫：只有 RUNNING 能出结果；已出结果的再来报，原样挡回、不做任何修改。 */
    private void requireRunning() {
        if (status != JobStatus.RUNNING) {
            throw new BizException("只有处理中（RUNNING）的任务才能上报结果，当前状态：" + status);
        }
    }

    /**
     * 领域行为：撤销任务。
     *
     * - 必须写明撤销原因；
     * - 只有 PENDING（还压在待处理里、没被节点领走）的任务可撤；
     *   RUNNING/SUCCESS/FAILED/CANCELLED 都不允许再撤。
     * - 原因落 errorMsg，finishedAt 记撤销时刻。
     *
     * 注意：这里只校验「当前看到的状态」；并发下可能刚被节点领走，
     * 所以仓储落库时还要带 status=PENDING 的条件再兜一道（乐观条件更新）。
     */
    public void cancel(String reason) {
        if (reason == null || reason.isBlank()) {
            throw new BizException("撤销原因不能为空");
        }
        if (status != JobStatus.PENDING) {
            throw new BizException("只有待处理（PENDING）的任务才能撤销，当前状态：" + status);
        }
        this.status = JobStatus.CANCELLED;
        this.errorMsg = reason.trim();
        this.finishedAt = LocalDateTime.now();
    }

    /**
     * 领域行为：重排（重试）一条失败的任务，把它放回待处理队列。
     *
     * - 只有 FAILED 能重排：还在排队等领的（PENDING）、正在跑的（RUNNING）、
     *   已成功的（SUCCESS）、已撤销的（CANCELLED）都不允许；
     * - 最多能跑几次在建任务时就定死了（maxAttempts）：已尝试次数到了上限的，
     *   给明确提示、不放进队列，免得到顶的单子没完没了占位置；
     * - 干干净净重新排队：上回留下的失败说明清掉、进度归零、起止时刻清空，
     *   已跑过的次数（attemptCount）原样保留 —— 下次被节点领走时 attemptNo 接着往下排。
     *
     * 注意：这里只校验「当前看到的状态」；同一条任务被连点几下、或几个人同时点，
     * 仓储落库时还要带 status=FAILED 的条件再兜一道（乐观条件更新，见 requeueIfFailed），
     * 保证只排一次、不重复入队。素材/档位是否还能用由应用层在重排前校验。
     */
    public void retry() {
        if (status != JobStatus.FAILED) {
            throw new BizException("只有失败（FAILED）的任务才能重试，当前状态：" + status);
        }
        if (attemptCount >= maxAttempts) {
            throw new BizException("任务已达到最大尝试次数（已尝试 " + attemptCount
                    + " 次，上限 " + maxAttempts + " 次），不能再重试");
        }
        this.status = JobStatus.PENDING;
        this.progress = 0;
        this.errorMsg = null;
        this.startedAt = null;
        this.finishedAt = null;
    }

    /** 聚合不变量：提交时要过这道校验。 */
    private void validate() {
        if (assetId == null) {
            throw new BizException("素材不能为空");
        }
        if (profileId == null) {
            throw new BizException("转码档位不能为空");
        }
        if (ownerDept == null || ownerDept.isBlank()) {
            throw new BizException("归属部门不能为空");
        }
        this.ownerDept = ownerDept.trim();
        if (priority < 1 || priority > 99) {
            throw new BizException("优先级需在 1-99 之间（越小越先做）");
        }
    }
}
