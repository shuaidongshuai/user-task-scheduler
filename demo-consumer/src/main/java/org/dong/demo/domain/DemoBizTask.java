package org.dong.demo.domain;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class DemoBizTask {
    private Long id;
    private String bizKey;
    private String status;
    private String payload;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
