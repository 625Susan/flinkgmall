package com.xly.gmall.realtime.beans;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class UserRegisterCountBean {
    private String stt;
    private String edt;
    private Long registerCt;
    private Long ts;
}
