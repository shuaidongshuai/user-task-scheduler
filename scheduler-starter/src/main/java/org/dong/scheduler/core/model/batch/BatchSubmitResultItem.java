package org.dong.scheduler.core.model.batch;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class BatchSubmitResultItem {
    private String clientTaskRef;
    private Long taskId;
    private String taskNo;
}
