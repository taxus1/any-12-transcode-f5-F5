package com.somepro.application.job;

import com.somepro.common.exception.BizException;
import com.somepro.domain.job.model.AttemptStatus;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * TranscodeJobAppService 编排规则单测：仓储全部打桩，不依赖 Spring / DB。
 */
class TranscodeJobAppServiceTest {

    private TranscodeJobRepository jobRepository;
    private MediaAssetRepository assetRepository;
    private TranscodeProfileRepository profileRepository;
    private TranscodeJobAppService service;

    @BeforeEach
    void setUp() {
        jobRepository = mock(TranscodeJobRepository.class);
        assetRepository = mock(MediaAssetRepository.class);
        profileRepository = mock(TranscodeProfileRepository.class);
        service = new TranscodeJobAppService(jobRepository, assetRepository, profileRepository);
    }

    private MediaAsset readyAsset() {
        MediaAsset asset = MediaAsset.register("MA-2026-0001", "a.mp4", "VIDEO",
                "mp4", 1024L, 60000L, null, "技术部");
        asset.setId(1L);
        return asset;
    }

    private TranscodeProfile enabledProfile() {
        TranscodeProfile profile = TranscodeProfile.register("PF-001", "高清", "VIDEO",
                "mp4", 1920, 1080, 4000, 128);
        profile.setId(2L);
        return profile;
    }

    private void stubAssetAndProfile(MediaAsset asset, TranscodeProfile profile) {
        when(assetRepository.findById(1L)).thenReturn(Mono.just(asset));
        when(profileRepository.findById(2L)).thenReturn(Mono.just(profile));
    }

    @Test
    void submitShouldFailWhenAssetMissing() {
        when(assetRepository.findById(1L)).thenReturn(Mono.empty());

        assertThrows(BizException.class, () -> service.submit(1L, 2L, "技术部", 1).block());
        verify(jobRepository, never()).submitNew(any());
    }

    @Test
    void submitShouldFailWhenAssetDoneOrDisabled() {
        MediaAsset done = readyAsset();
        done.setStatus(AssetStatus.DONE);
        when(assetRepository.findById(1L)).thenReturn(Mono.just(done));
        assertThrows(BizException.class, () -> service.submit(1L, 2L, "技术部", 1).block());

        MediaAsset disabled = readyAsset();
        disabled.setStatus(AssetStatus.DISABLED);
        when(assetRepository.findById(1L)).thenReturn(Mono.just(disabled));
        assertThrows(BizException.class, () -> service.submit(1L, 2L, "技术部", 1).block());

        verify(jobRepository, never()).submitNew(any());
    }

    @Test
    void submitShouldFailWhenDeptMismatch() {
        stubAssetAndProfile(readyAsset(), enabledProfile());

        assertThrows(BizException.class, () -> service.submit(1L, 2L, "别的部门", 1).block());
        verify(jobRepository, never()).submitNew(any());
    }

    @Test
    void submitShouldFailWhenProfileMissingOrDisabled() {
        when(assetRepository.findById(1L)).thenReturn(Mono.just(readyAsset()));
        when(profileRepository.findById(2L)).thenReturn(Mono.empty());
        assertThrows(BizException.class, () -> service.submit(1L, 2L, "技术部", 1).block());

        TranscodeProfile disabled = enabledProfile();
        disabled.setStatus(ProfileStatus.DISABLED);
        stubAssetAndProfile(readyAsset(), disabled);
        assertThrows(BizException.class, () -> service.submit(1L, 2L, "技术部", 1).block());

        verify(jobRepository, never()).submitNew(any());
    }

    @Test
    void submitShouldFailWhenMediaTypeMismatch() {
        MediaAsset audio = MediaAsset.register("MA-2026-0002", "a.mp3", "AUDIO",
                "mp3", 512L, 30000L, null, "技术部");
        audio.setId(1L);
        stubAssetAndProfile(audio, enabledProfile());

        assertThrows(BizException.class, () -> service.submit(1L, 2L, "技术部", 1).block());
        verify(jobRepository, never()).submitNew(any());
    }

    @Test
    void submitShouldFailWhenActiveJobExists() {
        stubAssetAndProfile(readyAsset(), enabledProfile());
        when(jobRepository.existsActiveByAssetAndProfile(1L, 2L)).thenReturn(Mono.just(Boolean.TRUE));

        BizException e = assertThrows(BizException.class,
                () -> service.submit(1L, 2L, "技术部", 1).block());
        assertEquals("该素材在此档位下已有未完成的转码任务（待处理或处理中），请勿重复提交", e.getMessage());
        verify(jobRepository, never()).submitNew(any());
    }

    @Test
    void submitShouldPersistPendingJob() {
        stubAssetAndProfile(readyAsset(), enabledProfile());
        when(jobRepository.existsActiveByAssetAndProfile(1L, 2L)).thenReturn(Mono.just(Boolean.FALSE));
        when(jobRepository.submitNew(any())).thenAnswer(inv -> {
            TranscodeJob job = inv.getArgument(0);
            job.setId(99L);
            job.setJobNo("TJ-2026-0001");
            return Mono.just(job);
        });

        TranscodeJob job = service.submit(1L, 2L, "技术部", null).block();

        assertEquals(JobStatus.PENDING, job.getStatus());
        assertEquals(TranscodeJob.DEFAULT_PRIORITY, job.getPriority());
        assertEquals("TJ-2026-0001", job.getJobNo());
        verify(jobRepository).submitNew(any());
    }

    @Test
    void cancelShouldFailWhenJobMissing() {
        when(jobRepository.findById(9L)).thenReturn(Mono.empty());

        assertThrows(BizException.class, () -> service.cancel(9L, "提错了").block());
        verify(jobRepository, never()).cancelIfPending(any());
    }

    @Test
    void cancelShouldPersistCancellation() {
        TranscodeJob job = TranscodeJob.submit(1L, 2L, "技术部", 1);
        job.setId(9L);
        job.setJobNo("TJ-2026-0001");
        when(jobRepository.findById(9L)).thenReturn(Mono.just(job));
        when(jobRepository.cancelIfPending(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));

        TranscodeJob cancelled = service.cancel(9L, "提错档位了").block();

        assertEquals(JobStatus.CANCELLED, cancelled.getStatus());
        assertEquals("提错档位了", cancelled.getErrorMsg());
        verify(jobRepository).cancelIfPending(any());
    }

    private TranscodeJob pendingJob() {
        TranscodeJob job = TranscodeJob.submit(1L, 2L, "技术部", 1);
        job.setId(9L);
        job.setJobNo("TJ-2026-0001");
        return job;
    }

    private TranscodeJob runningJob() {
        TranscodeJob job = pendingJob();
        job.claim();
        return job;
    }

    @Test
    void claimShouldFailWhenJobMissing() {
        when(jobRepository.findById(9L)).thenReturn(Mono.empty());

        assertThrows(BizException.class, () -> service.claim(9L, "WK-001").block());
        verify(jobRepository, never()).claimIfPending(any(), any());
    }

    @Test
    void claimShouldFailWhenAlreadyClaimedOrFinished() {
        // 已被领走的、已出结果的，再来领要挡回去
        for (JobStatus status : new JobStatus[]{JobStatus.RUNNING, JobStatus.SUCCESS, JobStatus.FAILED}) {
            TranscodeJob job = pendingJob();
            job.setStatus(status);
            when(jobRepository.findById(9L)).thenReturn(Mono.just(job));

            assertThrows(BizException.class, () -> service.claim(9L, "WK-001").block());
        }
        verify(jobRepository, never()).claimIfPending(any(), any());
    }

    @Test
    void claimShouldPersistRunningJobWithAttempt() {
        TranscodeJob job = pendingJob();
        when(jobRepository.findById(9L)).thenReturn(Mono.just(job));
        when(jobRepository.claimIfPending(any(), any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));

        TranscodeJob claimed = service.claim(9L, "WK-001").block();

        assertEquals(JobStatus.RUNNING, claimed.getStatus());
        assertEquals(1, claimed.getAttemptCount());
        assertNotNull(claimed.getStartedAt());
        // 执行记录跟着落库：第几次跑、哪台节点、几点开始
        verify(jobRepository).claimIfPending(any(), argThat(attempt ->
                attempt.getJobId().equals(9L) && attempt.getAttemptNo() == 1
                        && "WK-001".equals(attempt.getWorkerCode())
                        && attempt.getStatus() == AttemptStatus.RUNNING
                        && attempt.getStartedAt() != null));
    }

    @Test
    void claimShouldFailWhenWorkerCodeBlank() {
        TranscodeJob job = pendingJob();
        when(jobRepository.findById(9L)).thenReturn(Mono.just(job));

        assertThrows(BizException.class, () -> service.claim(9L, "  ").block());
        verify(jobRepository, never()).claimIfPending(any(), any());
    }

    @Test
    void reportProgressShouldFailWhenNotClaimed() {
        // 没被领走的任务不该收到进度
        when(jobRepository.findById(9L)).thenReturn(Mono.just(pendingJob()));

        assertThrows(BizException.class, () -> service.reportProgress(9L, 30).block());
        verify(jobRepository, never()).reportProgressIfRunning(any());
    }

    @Test
    void reportProgressShouldRejectRegression() {
        TranscodeJob job = runningJob();
        job.reportProgress(50);
        when(jobRepository.findById(9L)).thenReturn(Mono.just(job));

        assertThrows(BizException.class, () -> service.reportProgress(9L, 40).block());
        verify(jobRepository, never()).reportProgressIfRunning(any());
    }

    @Test
    void reportProgressShouldPersistForwardProgress() {
        TranscodeJob job = runningJob();
        when(jobRepository.findById(9L)).thenReturn(Mono.just(job));
        when(jobRepository.reportProgressIfRunning(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));

        TranscodeJob updated = service.reportProgress(9L, 60).block();

        assertEquals(60, updated.getProgress());
        verify(jobRepository).reportProgressIfRunning(any());
    }

    @Test
    void reportResultShouldRejectInvalidResultValue() {
        when(jobRepository.findById(9L)).thenReturn(Mono.just(runningJob()));

        assertThrows(BizException.class, () -> service.reportResult(9L, "OK", null, null, null).block());
        assertThrows(BizException.class, () -> service.reportResult(9L, " ", null, null, null).block());
        verify(jobRepository, never()).finishIfRunning(any());
    }

    @Test
    void reportResultShouldFailWhenNotRunning() {
        // 没被领走的任务不能出结果
        when(jobRepository.findById(9L)).thenReturn(Mono.just(pendingJob()));

        assertThrows(BizException.class, () -> service.reportResult(9L, "SUCCESS", null, null, null).block());
        verify(jobRepository, never()).finishIfRunning(any());
    }

    @Test
    void reportResultSuccessShouldPersistTerminalJob() {
        TranscodeJob job = runningJob();
        when(jobRepository.findById(9L)).thenReturn(Mono.just(job));
        when(jobRepository.finishIfRunning(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));

        TranscodeJob finished = service.reportResult(9L, "success", "/out/a.mp4", null, null).block();

        assertEquals(JobStatus.SUCCESS, finished.getStatus());
        assertEquals(100, finished.getProgress());
        assertEquals("/out/a.mp4", finished.getOutputPath());
        assertNotNull(finished.getFinishedAt());
        verify(jobRepository).finishIfRunning(any());
    }

    @Test
    void reportResultFailureShouldPersistTerminalJob() {
        TranscodeJob job = runningJob();
        when(jobRepository.findById(9L)).thenReturn(Mono.just(job));
        when(jobRepository.finishIfRunning(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));

        TranscodeJob finished = service.reportResult(9L, "FAILED", null, "转码器崩溃", null).block();

        assertEquals(JobStatus.FAILED, finished.getStatus());
        assertEquals("转码器崩溃", finished.getErrorMsg());
        assertNotNull(finished.getFinishedAt());
        verify(jobRepository).finishIfRunning(any());
    }

    @Test
    void reportResultShouldNotRewriteFinishedJob() {
        // 已出结果的任务，后面再怎么报都不该把它重新改一遍
        TranscodeJob job = runningJob();
        job.succeed("/out/a.mp4", null);
        when(jobRepository.findById(9L)).thenReturn(Mono.just(job));

        assertThrows(BizException.class, () -> service.reportResult(9L, "FAILED", null, "x", null).block());
        assertThrows(BizException.class, () -> service.reportProgress(9L, 10).block());
        assertEquals(JobStatus.SUCCESS, job.getStatus());
        verify(jobRepository, never()).finishIfRunning(any());
        verify(jobRepository, never()).reportProgressIfRunning(any());
    }

    @Test
    void listAttemptsShouldFailWhenJobMissing() {
        when(jobRepository.findById(9L)).thenReturn(Mono.empty());

        assertThrows(BizException.class, () -> service.listAttempts(9L).block());
        verify(jobRepository, never()).listAttempts(any());
    }

    @Test
    void listAttemptsShouldReturnRecords() {
        TranscodeJob job = runningJob();
        when(jobRepository.findById(9L)).thenReturn(Mono.just(job));
        JobAttempt attempt = JobAttempt.start(9L, 1, "WK-001", job.getStartedAt());
        when(jobRepository.listAttempts(9L)).thenReturn(Mono.just(List.of(attempt)));

        List<JobAttempt> attempts = service.listAttempts(9L).block();

        assertEquals(1, attempts.size());
        assertEquals("WK-001", attempts.get(0).getWorkerCode());
        assertEquals(AttemptStatus.RUNNING, attempts.get(0).getStatus());
    }

    private TranscodeJob failedJob() {
        TranscodeJob job = runningJob();
        job.fail("转码器崩溃", null);
        return job;
    }

    @Test
    void retryShouldFailWhenJobMissing() {
        when(jobRepository.findById(9L)).thenReturn(Mono.empty());

        assertThrows(BizException.class, () -> service.retry(9L).block());
        verify(jobRepository, never()).requeueIfFailed(any());
    }

    @Test
    void retryShouldFailWhenNotFailed() {
        // 还在排队的、正在跑的、成功的、撤掉的都不该点头（删除的列表翻不到，走 missing 分支）
        for (JobStatus status : new JobStatus[]{
                JobStatus.PENDING, JobStatus.RUNNING, JobStatus.SUCCESS, JobStatus.CANCELLED}) {
            TranscodeJob job = pendingJob();
            job.setStatus(status);
            when(jobRepository.findById(9L)).thenReturn(Mono.just(job));

            assertThrows(BizException.class, () -> service.retry(9L).block());
        }
        verify(jobRepository, never()).requeueIfFailed(any());
    }

    @Test
    void retryShouldFailWhenAttemptsExhausted() {
        TranscodeJob job = pendingJob();
        job.setStatus(JobStatus.FAILED);
        job.setAttemptCount(TranscodeJob.DEFAULT_MAX_ATTEMPTS);
        when(jobRepository.findById(9L)).thenReturn(Mono.just(job));

        BizException e = assertThrows(BizException.class, () -> service.retry(9L).block());
        assertEquals("任务已达到最多尝试次数（3 次），不能再重试", e.getMessage());
        // 到顶的单子不放回队列，不占位置
        verify(jobRepository, never()).requeueIfFailed(any());
    }

    @Test
    void retryShouldFailWhenAssetMissingOrDisabled() {
        when(jobRepository.findById(9L)).thenReturn(Mono.just(failedJob()));
        when(assetRepository.findById(1L)).thenReturn(Mono.empty());
        assertThrows(BizException.class, () -> service.retry(9L).block());

        MediaAsset disabled = readyAsset();
        disabled.setStatus(AssetStatus.DISABLED);
        when(assetRepository.findById(1L)).thenReturn(Mono.just(disabled));
        assertThrows(BizException.class, () -> service.retry(9L).block());

        verify(jobRepository, never()).requeueIfFailed(any());
    }

    @Test
    void retryShouldFailWhenProfileMissingOrDisabled() {
        when(jobRepository.findById(9L)).thenReturn(Mono.just(failedJob()));
        when(assetRepository.findById(1L)).thenReturn(Mono.just(readyAsset()));
        when(profileRepository.findById(2L)).thenReturn(Mono.empty());
        assertThrows(BizException.class, () -> service.retry(9L).block());

        TranscodeProfile disabled = enabledProfile();
        disabled.setStatus(ProfileStatus.DISABLED);
        when(profileRepository.findById(2L)).thenReturn(Mono.just(disabled));
        assertThrows(BizException.class, () -> service.retry(9L).block());

        verify(jobRepository, never()).requeueIfFailed(any());
    }

    @Test
    void retryShouldRequeueFailedJob() {
        TranscodeJob job = failedJob();
        when(jobRepository.findById(9L)).thenReturn(Mono.just(job));
        stubAssetAndProfile(readyAsset(), enabledProfile());
        when(jobRepository.requeueIfFailed(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));

        TranscodeJob requeued = service.retry(9L).block();

        // 干干净净重新排队：PENDING、进度 0、失败说明与起止时刻清空，已跑次数留着
        assertEquals(JobStatus.PENDING, requeued.getStatus());
        assertEquals(0, requeued.getProgress());
        assertEquals(null, requeued.getErrorMsg());
        assertEquals(null, requeued.getStartedAt());
        assertEquals(null, requeued.getFinishedAt());
        assertEquals(1, requeued.getAttemptCount());
        // 只重新排队一次；执行记录不在这里新增（没有 attempt 参数）
        verify(jobRepository).requeueIfFailed(any());
    }
}
