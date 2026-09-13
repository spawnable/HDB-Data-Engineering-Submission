# AWS ingestion and exploitation architecture

## Design position and assumptions

Use scheduled ECS/Fargate tasks to ingest files and run the existing Python ETL, S3 for all five data groups, Glue Data Catalog for metadata and Athena for SQL access from Tableau. The current data volume does not justify introducing Spark, a persistent cluster or a streaming platform. Ingestion and processing have separate roles and subnet policies even when they share a container image.

This is an **architecture proposal**, not a deployed AWS environment. Assume one AWS account, Singapore Region `ap-southeast-1`, two Availability Zones, and separate platform and Tableau VPCs. The same-account choice keeps the initial IAM and catalog model small; a cross-account extension is described below. HDB's exact CIDRs, account IDs, bucket names, corporate connectivity and approved Tableau versions have not been supplied.

For the assessment execution, only the uploaded CSVs are used. The public API in diagram 01 describes the requested future ingestion pattern; no download code or downloaded dataset contributes to the submitted results.

- [Diagram 01: batch ingestion PNG](../architecture/01_batch_ingestion.png) / [editable SVG](../architecture/01_batch_ingestion.svg)
- [Diagram 02: private analytics PNG](../architecture/02_private_analytics.png) / [editable SVG](../architecture/02_private_analytics.svg)

The diagrams use service/resource assets from the [official AWS architecture icon pack](https://aws.amazon.com/architecture/icons/). AZ-specific resources are shown as logical pairs to keep the diagrams readable; each AZ has its own applicable firewall endpoint, NAT and private endpoint ENIs. The internet gateway is a VPC attachment, illustrated in the egress zone rather than implying that it resides inside a subnet.

## 1. Batch ingestion, including files larger than 100 MB

### Sequence and ownership

1. **EventBridge Scheduler** starts a **Step Functions Standard** execution at the agreed batch cadence. Manual backfills use the same workflow with an explicit date range and source set. The scheduler target has a dead-letter queue and failure alarm.
2. The state machine starts an **ECS/Fargate ingestion task** in a private ingestion subnet with no public IP. Its role can write only the Raw landing prefixes and manifests, not curated data or query results.
3. The task discovers collection/dataset metadata, selects datasets whose coverage intersects the required scope, initiates each complete CSV download and polls with bounded backoff. Production configuration pins dataset identities and records metadata drift; it does not hardcode individual transactions or signed download URLs. Do not apply source row/column filters when the Raw contract requires original full files. The current public API describes an initiate/poll download flow and anonymous rate limits; use explicit timeouts, honor 429/Retry-After and make an optional API key a managed secret. [data.gov.sg download API](https://guide.data.gov.sg/developer-guide/dataset-apis/download-dataset)
4. Stream the response into bounded buffers and an **S3 multipart upload**, updating a SHA-256 digest without parsing or rewriting the CSV. Example tuning: 16 MiB parts and four concurrent part uploads, adjusted to memory and endpoint behavior. Check status, byte count, completion and checksum; do not use a multipart ETag as a universal MD5 checksum. Abort failed multipart uploads and apply a lifecycle rule to remove abandoned parts. AWS recommends considering multipart upload at approximately 100 MB. [S3 multipart guidance](https://docs.aws.amazon.com/AmazonS3/latest/userguide/tutorial-s3-mpu-additional-checksums.html)
5. Once all requested files are complete, publish a small **Raw manifest** listing dataset IDs, object keys, object version IDs, checksums, byte counts, retrieval time and source metadata. Avoid logging signed URL query strings. The workflow passes object references and manifest IDs, never file payloads, between states.
6. Start a separate **ETL Fargate task** in isolated processing subnets. It reads the committed manifest, applies the versioned Python rules, writes Cleaned, Transformed, Quarantined and Hashed to a new run prefix, and records the reference version and quality metrics. It has no default internet route.
7. Check schema, row conservation and identity constraints. On success, publish an immutable output manifest and update the serving view/catalog pointer to that version. Register known schemas through the **Glue API**. A crawler is optional for discovery, not the authority for accepting unexpected columns or types. A failed run leaves the previous serving version intact.

Fargate permits a longer-running, independently sized transfer process and bounded streaming without placing large files inside orchestration messages. Configure a task timeout long enough for the worst expected file, and size ephemeral storage for the image plus any spool/retry files and headroom. The base in-memory ETL should be replaced with an external-memory/distributed implementation if the *scoped* data exceed available memory; chunked reads alone do not make groupby/deduplication out of core. Fargate storage is configurable within documented limits. [Fargate storage](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/fargate-task-storage.html)

### Raw integrity and idempotence

Use versioned S3 objects and a manifest identity derived from the ordered input checksums, rule configuration and code/image version. Reprocessing the same identity yields the same business rows and hashes. An orchestration execution or single-writer lock prevents concurrent updates to the current serving pointer. Keep immutable run prefixes; do not append blindly or overwrite a partition while consumers read it.

A source change creates a new input version, even when its filename is unchanged. A task interrupted mid-transfer publishes no success manifest. An ETL failure is replayed from the existing committed Raw objects. S3 transfer checksums and the manifest give integrity/lineage; validation checks remain necessary after a successful transfer.

## 2. Public-source ingestion with private platform segmentation

The external data.gov.sg leg is necessarily public HTTPS. NAT does not turn that leg into a private connection. The design confines it to the ingestion path and keeps processing, storage access and analytics on private AWS service paths.

| Zone | Components | Allowed behavior |
|---|---|---|
| Public egress subnets | AZ-local NAT gateways; internet gateway attached to VPC | Outbound connections from approved ingestion workloads; no public task IPs or inbound application listeners |
| Dedicated inspection subnets | AWS Network Firewall endpoints | Stateful domain/TLS inspection policy, default deny except approved source API and download hosts |
| Private ingestion subnets | Download Fargate tasks | Source HTTPS through inspection/NAT; Raw S3 via gateway endpoint; platform APIs via interface endpoints |
| Isolated processing subnets | ETL Fargate tasks | No default internet route; approved S3 and AWS service endpoints only |
| Endpoint subnets | Interface endpoint ENIs in two AZs | Endpoint SGs allow only required workload SGs and service ports |

For the decentralized, same-AZ egress pattern in the diagram, the ingestion default route points to the local firewall endpoint, the firewall subnet's default route points to the local NAT, and the NAT subnet's default route points to the IGW. Add the more-specific return route for the ingestion subnet via the same firewall endpoint; firewall-to-ingestion traffic uses the VPC-local route. Match forward and return inspection paths. Do not place workloads in the firewall subnet. Review this routing with the network team rather than combining it with an unrelated TGW inspection pattern. [NAT and Network Firewall](https://docs.aws.amazon.com/whitepapers/latest/building-scalable-secure-multi-vpc-network-infrastructure/using-nat-gateway-with-firewall.html), [asymmetric-routing guidance](https://docs.aws.amazon.com/network-firewall/latest/developerguide/asymmetric-routing.html)

Allow only approved API and file-delivery domains discovered in trusted metadata. A returned URL is not automatically authorization for arbitrary internet access: require HTTPS, reject unapproved hosts/redirects and private/link-local destinations, and send unexpected endpoints for review. Verify TLS normally. A dedicated approved egress proxy is a reasonable substitute if that is HDB's existing standard; adding both a proxy and Network Firewall solely for this small workflow is unnecessary.

S3 access from VPC A uses its own gateway endpoint and bucket/prefix-scoped endpoint policy. Private Fargate image pulls need ECR API, ECR DKR and S3 access; allow the regional ECR layer-delivery S3 location as well as project buckets. Bake dependencies into the versioned image rather than running `pip install` at task startup. Use Logs, Glue and, where a direct client call requires it, KMS/Secrets Manager/STS interface endpoints. Task-role credential delivery is handled by ECS and does not inherently require an STS API call from the container. [Fargate endpoint dependencies](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/platform-version-migration.html)

Security groups are stateful; use narrowly scoped ingress and egress rules. If custom NACLs are used, include return ephemeral ports as well as service ports. Use VPC Resolver for DNS; enable DNS support/hostnames, private DNS on interface endpoints and controlled corporate DNS forwarding where relevant.

## 3. Private Tableau-to-Athena integration

### Connection path

Tableau Server runs on EC2 in **VPC B**, separate from VPC A. It reaches the regional Athena service through **Athena interface endpoints in VPC B**. No direct connection to the ETL instances is needed. With private DNS enabled, `athena.ap-southeast-1.amazonaws.com` resolves to the endpoint ENIs' private addresses. S3 and Athena are regional services, not resources residing inside VPC A. [Athena interface endpoints](https://docs.aws.amazon.com/athena/latest/ug/interface-vpc-endpoint.html)

Provide a **Glue interface endpoint** for driver/catalog metadata access and a **separate S3 gateway endpoint in VPC B** for result downloads and any permitted direct S3 reads. Associate the S3 endpoint with every Tableau worker/backgrounder route table that executes queries or refreshes. A gateway endpoint cannot be shared through VPC peering or TGW, so reusing VPC A's S3 endpoint from VPC B would not work. This design requires no VPC peering merely to access the same Athena workgroup or S3 bucket. [S3 gateway endpoint constraints](https://docs.aws.amazon.com/vpc/latest/privatelink/vpc-endpoints-s3.html)

| Caller -> destination | Transport | Control |
|---|---|---|
| Tableau SG -> Athena endpoint SG | TLS/TCP 443 | Query submission, status and metadata; endpoint policy limits principals/actions |
| Tableau SG -> Athena endpoint SG | TLS/TCP 444 | Driver streaming results; permit `athena:GetQueryResultsStream` |
| Tableau SG -> Glue endpoint SG | TLS/TCP 443 | Approved catalog/database/table metadata |
| Tableau -> VPC B S3 gateway -> result objects | HTTPS/TCP 443 | S3 managed-prefix-list route, endpoint policy and result-prefix permissions |
| Tableau -> regional STS endpoint, if assuming a role | TLS/TCP 443 | Short-lived credential exchange; configure regional STS |
| ETL -> VPC A S3 gateway -> curated objects | HTTPS/TCP 443 | Producer role, separate endpoint and write prefix |

S3 gateway endpoints do not have security groups. For interface endpoints, permit inbound service ports from the Tableau SG, and restrict Tableau egress accordingly. Both Tableau's connector documentation and Athena's driver documentation identify 443/444 considerations; allowing only 443 can cause result-stream failures. [Tableau Athena connector](https://help.tableau.com/current/pro/desktop/en-us/examples_amazonathena.htm), [Athena JDBC requirements](https://docs.aws.amazon.com/athena/latest/ug/jdbc-v3-driver.html)

Account for **both result-fetch modes**. Some driver paths stream through Athena on 444, while JDBC 3.x can fetch results directly from S3 on 443. The design supports both, so query submission succeeding does not conceal a blocked result path. The explicit fetcher choice and property names depend on the installed driver. [Athena result-fetch settings](https://docs.aws.amazon.com/athena/latest/ug/jdbc-v3-driver-advanced-connection-parameters.html)

### Driver, identity and data-source settings

Install the Tableau-supported Athena driver on each node that may run VizQL, backgrounder, refresh or query workloads. Keep the connector and driver versions consistent. Use the built-in Amazon Athena connector and the selected Region, catalog, database, workgroup and S3 staging/result location.

| Setting | Proposed value / responsibility |
|---|---|
| Server | `athena.ap-southeast-1.amazonaws.com` with private DNS |
| Region | `ap-southeast-1` |
| Catalog / database | `AwsDataCatalog` / approved HDB resale database |
| Workgroup | Dedicated `hdb-resale-bi` workgroup |
| Results | Dedicated approved S3 prefix, enforced by workgroup configuration |
| Encryption | SSE-KMS, enforced by workgroup and bucket configuration |
| Authentication | Dedicated least-privilege EC2 instance role via a supported JDBC instance-profile provider |

The credential design assumes a supported Tableau/driver combination can select the instance-profile provider through its connection/properties configuration. AWS JDBC 3.x documents `CredentialsProvider=InstanceProfile`, which obtains temporary credentials from EC2 instance metadata; verify that the chosen Tableau connector honors that setting, rather than assuming every driver release does. Require IMDSv2 and avoid permanent keys in workbooks or the repository. A failure of this compatibility check requires an approved identity design before go-live, such as Tableau's supported federated OAuth/OIDC flow; that alternative must separately account for IdP connectivity and unattended refreshes. [Instance-profile credentials](https://docs.aws.amazon.com/athena/latest/ug/jdbc-v3-driver-instance-profile-credentials.html), [Tableau Athena IAM OAuth](https://help.tableau.com/current/pro/desktop/en-us/amazon_athena_idp.htm)

The baseline uses a shared Tableau service role: it grants a common dataset view to that audience and does not pretend to propagate every viewer's AWS identity. If different teams require different rows/columns, introduce appropriately separated roles/data sources or an approved Lake Formation/federated identity design. Network isolation alone is not row-level authorization.

### Permissions and the service-to-service distinction

Grant the consumer role only the necessary Athena execution/status/result actions in the dedicated workgroup, Glue read metadata, curated S3 read prefixes, result S3 read/write locations required for query execution, and permitted KMS keys. Producers, stewards and consumers have distinct roles; the normal Tableau role cannot inspect Raw or Quarantined. Endpoint policies supplement IAM and bucket/key policies; they do not grant missing IAM permissions.

Athena accesses S3 using the caller's permissions. Those service-to-service operations run on the AWS network and **do not traverse the client's VPC endpoint**. A bucket policy that unconditionally denies all requests missing `aws:SourceVpce` can break Athena. Restrict direct client access to approved endpoints, while permitting the intended Athena forward-access-session path for approved roles/resources, using the appropriate `aws:CalledVia`/service conditions. Test the actual policy intersection; a broad `ViaAWSService` exception by itself is not a least-privilege allow statement. [S3 permissions for Athena](https://docs.aws.amazon.com/athena/latest/ug/s3-permissions.html), [CalledVia for Athena](https://docs.aws.amazon.com/athena/latest/ug/security-iam-athena-calledvia.html), [forward access sessions](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_forward_access_sessions.html)

Treat query results as governed data. A user with S3 access to result objects may read them independently of Athena query-result APIs. Restrict result prefixes, retention and KMS decrypt permissions. A workgroup alone is not guaranteed per-user result isolation; use separate workgroups/prefixes/roles where audiences differ. Block S3 public access, disable unnecessary ACLs with bucket-owner enforcement, and require TLS and approved encryption settings.

## 4. Performance, scalability and maintainability

- Use typed, compressed Parquet and explicit schemas. For this modest volume, a compact dataset or coarse year partitions is adequate. Introduce month partitions and 128-256 MiB target files only when volume supports them; do not generate thousands of tiny files. Partition pruning and column projection reduce Athena scans.
- Publish stable curated views for Tableau. Hide lineage and internal validation attributes from default business-facing data sources while retaining them in the engineering outputs. Rename the assessment's display-style identifier column deliberately if adopting snake_case in the serving schema.
- Keep full immutable snapshot runs for this assessment-sized dataset. For larger incremental feeds, reprocess affected months/partitions and preserve the same maximum-price/key policy, versioned reference and reconciliation checks. Recompute any group means and affected identifiers; do not assume they remain constant when a group changes.
- Bound Athena concurrency and scan usage through workgroup settings/alarms. Use Tableau extracts where refresh cadence permits, or filter live queries by period and project only needed columns. Validate whether query-result reuse is acceptable for the freshness requirement.
- Keep code, tests, rule configuration, container digest and schema version in source control. Infrastructure is managed with IaC in an operational implementation. A rule change is a reviewed versioned release, not an in-place notebook edit.
- A persistent Airflow deployment, Spark cluster, Kinesis stream or separate database is unnecessary for this simple batch source. Consider Glue/Spark or another out-of-core engine only when measured workload size, duration or partitioning complexity warrants it.

## 5. Operations, recovery and release checks

Record source-to-output row counts, validation reasons, missingness, unseen domains, duplicate rate, anomaly rate, unassessed count, processing duration, bytes and input/manifest versions. Compare with a documented baseline and fail publication on schema breaks, reconciliation failure, hash collisions or an approved quality threshold. Because March-May 2012 fail the frozen reference by design, this historical reference should not be silently treated as an evergreen production gate.

CloudWatch logs/alarms cover task failures, download failures, retry exhaustion, missed schedules, runtime and quality drift. CloudTrail, selected S3 data events, VPC Flow Logs and Network Firewall logs support audit. Avoid full record dumps or credential/signed-URL leakage in operational logs. Retain raw data and quarantined evidence according to the approved retention policy, with lifecycle rules for intermediate files and query results.

Before deployment acceptance, test:

1. A successful batch plus a file over 100 MB, partial transfer failure, retry exhaustion and replay of an already processed input. Confirm original bytes/checksum and no visible partial publication.
2. An unknown schema/category and a deliberately anomalous record. Confirm quarantine reason, row conservation, previous-version availability and steward review/release path.
3. Tableau DNS resolution to private Athena/Glue ENIs; direct S3 access through VPC B's gateway route; successful query **and** result retrieval on the selected fetch path.
4. Live-query and background refresh behavior on all relevant Tableau nodes; instance-role credential refresh; approved driver settings and result encryption.
5. Negative permissions: unrelated role, workgroup, bucket/prefix, KMS key, unapproved egress host and access from outside the approved network. Confirm no public path is needed for query/result traffic.
6. An AZ/task failure and a failed catalog publication. Confirm recovery from immutable manifests without duplicate outputs or mixed versions.

These are proposed acceptance tests, not test results from the build host. The local ETL tests and executed notebook are the implemented evidence for Part 1.

## 6. Cross-account extension

If the platform and Tableau are in different AWS accounts, retain an Athena/S3 endpoint set in the Tableau VPC. Add an explicit role trust/AssumeRole path, regional STS endpoint, resource policies for the target catalog and buckets, and KMS key grants. Choose deliberately whether the workgroup/catalog lives in the producer account with an assumed role or is exposed via supported cross-account catalog/Lake Formation sharing. Do not mix those two models without a reason. Endpoint routing does not confer cross-account authorization, and a TGW attachment alone does not share a gateway endpoint.

Corporate user access to Tableau, software patching, license activation and federated IdP access are separate network flows. Their required connectivity must be assessed independently; the private query-path claim applies to Tableau, Athena, Glue and S3 data/result traffic.
