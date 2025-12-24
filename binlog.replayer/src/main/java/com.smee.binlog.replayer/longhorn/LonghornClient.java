package com.smee.binlog.replayer.longhorn;

import com.smee.binlog.replayer.ApplicationConfig;
import lombok.var;
import okhttp3.OkHttpClient;
import okhttp3.logging.HttpLoggingInterceptor;
import retrofit2.Call;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;
import retrofit2.http.*;

import java.util.concurrent.TimeUnit;

public interface LonghornClient {

    @POST("/v1/volumes/{name}?action=snapshotCreate")
    Call<CreateSnapshotResponse> createSnapshot(@Path("name") String name, @Body CreateSnapshotRequest body);

    @POST("/v1/volumes/{name}?action=snapshotList")
    Call<SnapshotListResponse> snapshotList(@Path("name") String name);

    public static LonghornClient build(ApplicationConfig.LonghornConfig config) {
        var log = new HttpLoggingInterceptor();
        log.setLevel(HttpLoggingInterceptor.Level.BODY);
        var hc = new OkHttpClient.Builder();
        hc.connectTimeout(config.getApiTimeout(), TimeUnit.SECONDS)
                .writeTimeout(config.getApiTimeout(), TimeUnit.SECONDS)
                .readTimeout(config.getApiTimeout(), TimeUnit.SECONDS)
                .addNetworkInterceptor(new LonghornRedirectFixInterceptor())
                .addInterceptor(log);
        return new Retrofit.Builder()
                .baseUrl(config.getEndpoint())
                .addConverterFactory(JacksonConverterFactory.create())
                .client(hc.build())
                .build()
                .create(LonghornClient.class);
    }
}