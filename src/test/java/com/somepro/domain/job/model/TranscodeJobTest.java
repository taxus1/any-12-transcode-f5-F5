package com.somepro.domain.job.model;

import com.somepro.common.exception.BizException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * TranscodeJob 领域规则单测（纯领域，不依赖 Spring / DB）。
 */
class TranscodeJobTest {

    @Test
    void submitShouldInitPendingJob() {
        TranscodeJob job = TranscodeJob.submit(1L, 2L, " 技术部 ", null);

        assertEquals(JobStatus.PENDING, job.getStatus());
        assertEquals(TranscodeJob.DEFAULT_PRIORITY, job.getPriority());
        assertEquals(0, job.getAttemptCount());
        assertEquals(TranscodeJob.DEFAULT_MAX_ATTEMPTS, job.getMaxAttempts());
        assertEquals(0, job.getProgress());
        assertEquals("技术部", job.getOwnerDept());
        assertNotNull(job.getSubmittedAt());
    }

    @Test
    void submitShouldValidateRequiredFields() {
        assertThrows(BizException.class, () -> TranscodeJob.submit(null, 2L, "技术部", 1));
        assertThrows(BizException.class, () -> TranscodeJob.submit(1L, null, "技术部", 1));
        assertThrows(BizException.class, () -> TranscodeJob.submit(1L, 2L, " ", 1));
        assertThrows(BizException.class, () -> TranscodeJob.submit(1L, 2L, "技术部", 0));
        assertThrows(BizException.class, () -> TranscodeJob.submit(1L, 2L, "技术部", 100));
    }

    @Test
    void cancelShouldRequireReason() {
        TranscodeJob job = TranscodeJob.submit(1L, 2L, "技术部", 1);

        assertThrows(BizException.class, () -> job.cancel(null));
        assertThrows(BizException.class, () -> job.cancel("  "));
    }

    @Test
    void cancelShouldOnlyAllowPending() {
        TranscodeJob job = TranscodeJob.submit(1L, 2L, "技术部", 1);
        job.setStatus(JobStatus.RUNNING);

        BizException e = assertThrows(BizException.class, () -> job.cancel("提错了"));
        assertEquals("只有待处理（PENDING）的任务才能撤销，当前状态：RUNNING", e.getMessage());
    }

    @Test
    void cancelShouldMarkCancelledWithReason() {
        TranscodeJob job = TranscodeJob.submit(1L, 2L, "技术部", 1);

        job.cancel(" 提错档位了 ");

        assertEquals(JobStatus.CANCELLED, job.getStatus());
        assertEquals("提错档位了", job.getErrorMsg());
        assertNotNull(job.getFinishedAt());
    }

    @Test
    void claimShouldTransitPendingToRunning() {
        TranscodeJob job = TranscodeJob.submit(1L, 2L, "技术部", 1);

        job.claim();

        assertEquals(JobStatus.RUNNING, job.getStatus());
        assertEquals(1, job.getAttemptCount());
        assertEquals(0, job.getProgress());
        assertNotNull(job.getStartedAt());
    }

    @Test
    void claimShouldRejectNonPending() {
        // 已被领走的、已出结果的、已撤销的，再来领都要挡回去
        for (JobStatus status : new JobStatus[]{
                JobStatus.RUNNING, JobStatus.SUCCESS, JobStatus.FAILED, JobStatus.CANCELLED}) {
            TranscodeJob job = TranscodeJob.submit(1L, 2L, "技术部", 1);
            job.setStatus(status);

            BizException e = assertThrows(BizException.class, job::claim);
            assertEquals("只有待处理（PENDING）的任务才能被节点领取，当前状态：" + status, e.getMessage());
        }
    }

    @Test
    void reportProgressShouldRejectWhenNotClaimed() {
        // 没被领走的任务不该收到进度
        TranscodeJob job = TranscodeJob.submit(1L, 2L, "技术部", 1);

        BizException e = assertThrows(BizException.class, () -> job.reportProgress(10));
        assertEquals("只有处理中（RUNNING）的任务才能上报进度，当前状态：PENDING", e.getMessage());
        assertEquals(0, job.getProgress());
    }

    @Test
    void reportProgressShouldValidateRange() {
        TranscodeJob job = claimedJob();

        assertThrows(BizException.class, () -> job.reportProgress(null));
        assertThrows(BizException.class, () -> job.reportProgress(-1));
        assertThrows(BizException.class, () -> job.reportProgress(101));
    }

    @Test
    void reportProgressShouldOnlyMoveForward() {
        TranscodeJob job = claimedJob();

        job.reportProgress(50);
        assertEquals(50, job.getProgress());

        // 报一样的不算回退，放行
        job.reportProgress(50);
        assertEquals(50, job.getProgress());

        // 报得比上一次小，挡回去
        BizException e = assertThrows(BizException.class, () -> job.reportProgress(40));
        assertEquals("进度只能往前，不能回退：当前已 50%，上报 40%", e.getMessage());
        assertEquals(50, job.getProgress());

        job.reportProgress(100);
        assertEquals(100, job.getProgress());
    }

    @Test
    void succeedShouldMarkTerminalWithFinishedAt() {
        TranscodeJob job = claimedJob();
        job.reportProgress(80);

        job.succeed(" /out/a.mp4 ", null);

        assertEquals(JobStatus.SUCCESS, job.getStatus());
        assertEquals(100, job.getProgress());
        assertEquals("/out/a.mp4", job.getOutputPath());
        assertNotNull(job.getFinishedAt());
    }

    @Test
    void failShouldMarkTerminalWithReason() {
        TranscodeJob job = claimedJob();

        job.fail(" 转码器崩溃 ", null);

        assertEquals(JobStatus.FAILED, job.getStatus());
        assertEquals("转码器崩溃", job.getErrorMsg());
        assertNotNull(job.getFinishedAt());
    }

    @Test
    void retryShouldOnlyAllowFailed() {
        // 还在排队等领的、正在跑的、已成功的、被撤掉的，都不该能重试
        for (JobStatus status : new JobStatus[]{
                JobStatus.PENDING, JobStatus.RUNNING, JobStatus.SUCCESS, JobStatus.CANCELLED}) {
            TranscodeJob job = TranscodeJob.submit(1L, 2L, "技术部", 1);
            job.setStatus(status);

            BizException e = assertThrows(BizException.class, job::retry);
            assertEquals("只有失败（FAILED）的任务才能重试，当前状态：" + status, e.getMessage());
        }
    }

    @Test
    void retryShouldRejectWhenAttemptLimitReached() {
        // 能重排几次建任务时就定死（maxAttempts），到了上限给明确话、不再放回队列
        TranscodeJob job = failedJob(3);
        job.setMaxAttempts(3);

        BizException e = assertThrows(BizException.class, job::retry);
        assertEquals("任务已达到最大尝试次数（已尝试 3 次，上限 3 次），不能再重试", e.getMessage());
        assertEquals(JobStatus.FAILED, job.getStatus());
    }

    @Test
    void retryShouldCleanFieldsButKeepAttemptCount() {
        // 失败说明清掉、进度归零、起止时刻清空，已跑过的次数留着不动，干干净净回 PENDING
        TranscodeJob job = failedJob(2);
        assertEquals(80, job.getProgress());

        job.retry();

        assertEquals(JobStatus.PENDING, job.getStatus());
        assertEquals(0, job.getProgress());
        assertNull(job.getErrorMsg());
        assertNull(job.getStartedAt());
        assertNull(job.getFinishedAt());
        assertEquals(2, job.getAttemptCount());
        assertEquals(TranscodeJob.DEFAULT_MAX_ATTEMPTS, job.getMaxAttempts());
    }

    @Test
    void retryThenClaimShouldContinueAttemptNumber() {
        // 重试只是放回队列、不新增执行记录；再次被领走时执行序号接着上一次往下排
        TranscodeJob job = failedJob(2);

        job.retry();
        job.claim();

        assertEquals(JobStatus.RUNNING, job.getStatus());
        assertEquals(3, job.getAttemptCount());
    }

    @Test
    void finishShouldRejectWhenNotRunning() {
        // 没被领走的任务不能出结果
        TranscodeJob pending = TranscodeJob.submit(1L, 2L, "技术部", 1);
        assertThrows(BizException.class, () -> pending.succeed(null, null));
        assertThrows(BizException.class, () -> pending.fail("x", null));

        // 已出结果的任务，后面再怎么报都不该把它重新改一遍
        TranscodeJob job = claimedJob();
        job.succeed("/out/a.mp4", null);
        assertThrows(BizException.class, () -> job.succeed("/out/b.mp4", null));
        assertThrows(BizException.class, () -> job.fail("又失败了", null));
        assertThrows(BizException.class, () -> job.reportProgress(10));
        assertEquals(JobStatus.SUCCESS, job.getStatus());
        assertEquals(100, job.getProgress());
        assertEquals("/out/a.mp4", job.getOutputPath());
    }

    /** 造一个已被节点领取的 RUNNING 任务。 */
    private static TranscodeJob claimedJob() {
        TranscodeJob job = TranscodeJob.submit(1L, 2L, "技术部", 1);
        job.claim();
        return job;
    }

    /** 造一条已经跑过 attemptCount 次、当前 FAILED 的任务（每次报 80% 后失败，中间按流程重试回队）。 */
    private static TranscodeJob failedJob(int attemptCount) {
        TranscodeJob job = TranscodeJob.submit(1L, 2L, "技术部", 1);
        for (int i = 0; i < attemptCount; i++) {
            if (i > 0) {
                job.retry();
            }
            job.claim();
            job.reportProgress(80);
            job.fail("转码器崩溃", null);
        }
        return job;
    }
}
