package site.holliverse.worker.batch.consultation.model;

public record AnalysisRequestMessage(
        String dispatchRequestId,
        long caseId,
        long analyzerVersion
) {
}
