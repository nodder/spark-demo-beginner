package com.bankcomm.pccc.onesight.demo.sparkstreaming.phase2;

import java.io.Serializable;

public interface FunctionS <F, T> extends Serializable 
{
    T apply(F f);
}
