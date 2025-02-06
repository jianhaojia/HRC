package org.HFC.SQM.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Packager {
    private int id;
    private LocalDateTime time;
    private int production;
    private String status;
    private double uptime;
}
