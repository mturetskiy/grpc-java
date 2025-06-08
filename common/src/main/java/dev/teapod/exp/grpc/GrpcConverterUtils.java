package dev.teapod.exp.grpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import dev.teapod.exp.grpc.oms.OmsOuterClass;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.time.Instant;

public class GrpcConverterUtils {
    public static Timestamp fromInstant(Instant ts) {
        return Timestamp.newBuilder()
                .setSeconds(ts.getEpochSecond())
                .setNanos(ts.getNano())
                .build();
    }

    public static Instant fromTimestamp(Timestamp ts) {
        return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
    }

    public static OmsOuterClass.DecimalValue fromBigDecimal(BigDecimal bd) {
        return OmsOuterClass.DecimalValue.newBuilder()
                .setScale(bd.scale())
                .setPrecision(bd.precision())
                .setValue(ByteString.copyFrom(bd.unscaledValue().toByteArray()))
                .build();
    }

    public static BigDecimal fromDecimalValue(OmsOuterClass.DecimalValue dv) {
        BigInteger bigInteger = new BigInteger(dv.getValue().toByteArray());
        MathContext mc = new MathContext(dv.getPrecision());
        return new BigDecimal(bigInteger, dv.getScale(), mc);
    }
}
