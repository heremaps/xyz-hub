package com.here.naksha.storage.http;

import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static org.junit.jupiter.api.Assertions.*;

@WireMockTest(httpPort = RequestSenderTest.MOCK_PORT)
class RequestSenderTest {

    public static final int MOCK_PORT = 9100;
    private static final String MOCK_URL = "http://localhost:"+MOCK_PORT;
    private static final String MOCK_ENDPOINT = "/my_env/my_storage/my_feat_type/features";

    @Mock
    CompletableFuture<HttpResponse<byte[]>> mockFuture;
    @Mock
    HttpClient httpClientMock;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void t01_testGoAwayException() throws ExecutionException, InterruptedException, TimeoutException {
        // Validate that RequestSender makes retry attempt on GOAWAY exception
        // Given: Storage properties with maxRetries allowed
        final RequestSender.KeyProperties keyProps = new RequestSender.KeyProperties(
                "local_storage_test_id",
                MOCK_URL,
                new HashMap<>(),
                10,
                10,
                1
        );

        // Given: Mock setup for generating GOAWAY exception on first attempt
        // Mock HttpClient on first factory method call, and then return real object on next call
        final MockedStatic<HttpClientFactory> httpClientFactoryMock = Mockito.mockStatic(HttpClientFactory.class);
        httpClientFactoryMock.when(() -> HttpClientFactory.getHttpClient(Mockito.any())).thenReturn(httpClientMock).thenCallRealMethod();
        // Throw GOAWAY exception when mockFuture is used
        Mockito.doThrow(new ExecutionException(new IOException("GOAWAY"))).when(mockFuture).get(Mockito.anyLong(), Mockito.any());
        // The first (mocked) HttpClient will return mockFuture (which will in turn throw GOAWAY exception)
        Mockito.doReturn(mockFuture).when(httpClientMock).sendAsync(Mockito.any(), Mockito.any());

        // Given: new RequestSender object
        final RequestSender sender = new RequestSender(keyProps);

        // Given: WireMock stubbed success response when real HttpClient is used (i.e. on retry attempt)
        final UrlPattern endpointPath = urlPathEqualTo(MOCK_ENDPOINT);
        stubFor(get(endpointPath)
                .willReturn(ok()));

        // When: the Http request is actually send to mock endpoint
        final HttpResponse<byte[]> response = sender.sendRequest(MOCK_ENDPOINT, null);

        // Then: Perform assertions

        // Then: Verify request reached endpoint once
        verify(1, getRequestedFor(endpointPath));
        // Verify success code 200 (eventhough in first attempt GOAWAY exception was raised)
        assertEquals(200, response.statusCode(), "Http status code doesn't match");
    }

}