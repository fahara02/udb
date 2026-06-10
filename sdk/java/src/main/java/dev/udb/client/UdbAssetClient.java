package dev.udb.client;

import com.udb.core.asset.services.v1.AssetServiceGrpc;
import com.udb.core.asset.services.v1.CompleteStepRequest;
import com.udb.core.asset.services.v1.CompleteStepResponse;
import com.udb.core.asset.services.v1.CreatePipelineDefinitionRequest;
import com.udb.core.asset.services.v1.CreatePipelineDefinitionResponse;
import com.udb.core.asset.services.v1.GetAssetRequest;
import com.udb.core.asset.services.v1.GetAssetResponse;
import com.udb.core.asset.services.v1.GetPipelineDefinitionRequest;
import com.udb.core.asset.services.v1.GetPipelineDefinitionResponse;
import com.udb.core.asset.services.v1.GetPipelineRequest;
import com.udb.core.asset.services.v1.GetPipelineResponse;
import com.udb.core.asset.services.v1.ListAssetsRequest;
import com.udb.core.asset.services.v1.ListAssetsResponse;
import com.udb.core.asset.services.v1.RegisterAssetRequest;
import com.udb.core.asset.services.v1.RegisterAssetResponse;
import com.udb.core.asset.services.v1.StartPipelineRequest;
import com.udb.core.asset.services.v1.StartPipelineResponse;
import io.grpc.Channel;

/**
 * Blocking facade over the native {@code AssetService} (asset registration +
 * processing pipelines). Rides the shared control-plane channel and attaches the
 * project {@link UdbMetadata} headers to every call. The raw generated stub stays
 * reachable via {@link #stub()}.
 */
public final class UdbAssetClient {
  private final AssetServiceGrpc.AssetServiceBlockingStub stub;

  UdbAssetClient(Channel channel, UdbMetadata metadata) {
    this(channel, metadata, UdbCredentials.fromMetadata(metadata));
  }

  UdbAssetClient(Channel channel, UdbMetadata metadata, UdbCredentials credentials) {
    this.stub =
        AssetServiceGrpc.newBlockingStub(channel)
            .withInterceptors(UdbClient.credentialInterceptor(metadata, credentials));
  }

  /** The raw generated blocking stub (never hidden). */
  public AssetServiceGrpc.AssetServiceBlockingStub stub() {
    return stub;
  }

  /** Define a reusable processing pipeline (ordered steps). */
  public CreatePipelineDefinitionResponse createPipelineDefinition(
      CreatePipelineDefinitionRequest request) {
    return stub.createPipelineDefinition(request);
  }

  /** Fetch a pipeline definition by id. */
  public GetPipelineDefinitionResponse getPipelineDefinition(
      GetPipelineDefinitionRequest request) {
    return stub.getPipelineDefinition(request);
  }

  /** Register an asset (typically backed by a stored file). */
  public RegisterAssetResponse registerAsset(RegisterAssetRequest request) {
    return stub.registerAsset(request);
  }

  /** Start a pipeline instance over an asset. */
  public StartPipelineResponse startPipeline(StartPipelineRequest request) {
    return stub.startPipeline(request);
  }

  /** Fetch a running/finished pipeline instance by id. */
  public GetPipelineResponse getPipeline(GetPipelineRequest request) {
    return stub.getPipeline(request);
  }

  /** Mark a pipeline step complete (advances the instance). */
  public CompleteStepResponse completeStep(CompleteStepRequest request) {
    return stub.completeStep(request);
  }

  /** List assets for the tenant, with paging/filter on the request. */
  public ListAssetsResponse listAssets(ListAssetsRequest request) {
    return stub.listAssets(request);
  }

  /** Fetch an asset's metadata by id. */
  public GetAssetResponse getAsset(GetAssetRequest request) {
    return stub.getAsset(request);
  }
}
