package site.holliverse.worker.batch.consultation.model;

public record DispatchTarget(
        long analysisId,
        long caseId,
        long analyzerVersion
) {
}
