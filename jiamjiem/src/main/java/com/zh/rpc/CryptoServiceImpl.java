package com.zh.rpc;

import com.zh.DataSecureUtils;
import io.grpc.stub.StreamObserver;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

/**
 * @Package com.zh.rpc.CryptoServiceImpl
 * @Author zhou.han
 * @Date 2025/8/3 13:33
 * @description:
 */
public class CryptoServiceImpl extends CryptoServiceGrpc.CryptoServiceImplBase {

    private final DataSecureUtils cryptoUtil;

    public CryptoServiceImpl() {
        SecretKey secretKey = new SecretKeySpec(DataSecureUtils.keyBytes, "AES");
        this.cryptoUtil = new DataSecureUtils(secretKey);
    }

    @Override
    public void encrypt(CryptoProto.EncryptRequest request, StreamObserver<CryptoProto.EncryptResponse> responseObserver) {
        try {
            String plainText = request.getPlainText();
            byte[] encryptedData = cryptoUtil.encrypt(plainText);

            CryptoProto.EncryptResponse response = CryptoProto.EncryptResponse.newBuilder()
                    .setEncryptedData(com.google.protobuf.ByteString.copyFrom(encryptedData))
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void decrypt(CryptoProto.DecryptRequest request, StreamObserver<CryptoProto.DecryptResponse> responseObserver) {
        try {
            byte[] encryptedData = request.getEncryptedData().toByteArray();
            String decryptedText = cryptoUtil.decrypt(encryptedData);

            CryptoProto.DecryptResponse response = CryptoProto.DecryptResponse.newBuilder()
                    .setDecryptedText(decryptedText)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}
