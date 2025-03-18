package fr.distrimind.oss.asqlconnector;

import java.math.BigDecimal;

class ASLConnectorBytesArrayType {
    static final byte NULL_TYPE=0;
    static final byte BYTES_ARRAY_TYPE=1;
    static final byte BIG_DECIMAL_TYPE=2;
    static final byte UNKNOWN_TYPE=4;
    static byte getArrayType(Object o) {
        if (o == null)
            return NULL_TYPE;
        if (o.getClass() == byte[].class)
            return BYTES_ARRAY_TYPE;
        else if (o instanceof BigDecimal)
            return BIG_DECIMAL_TYPE;
        else
            return UNKNOWN_TYPE;
    }
}
