package com.smee.binlog.replayer.longhorn;

import lombok.var;
import okhttp3.Interceptor;
import okhttp3.Response;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

public class LonghornRedirectFixInterceptor implements Interceptor {
    @NotNull
    @Override
    public Response intercept(@NotNull Chain chain) throws IOException {
        var res = chain.proceed(chain.request());
        if (res.code() == 301) {
            res = res.newBuilder()
                    .code(200)
                    .build();
        }
        return res;
    }
}