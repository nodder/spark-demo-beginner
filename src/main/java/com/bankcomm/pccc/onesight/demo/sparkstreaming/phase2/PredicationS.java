package com.bankcomm.pccc.onesight.demo.sparkstreaming.phase2;

import java.io.Serializable;

public interface PredicationS <T> extends Serializable
{
    boolean test(T t);
}
