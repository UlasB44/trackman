# Trackman SPCS Deployment

Snowpark Container Services deployment for Trackman dashboards.

## Live Endpoints

- **Main Analytics**: https://ir73uh-sfseeurope-ulas-aws-us-west-2.snowflakecomputing.app
- **Financial Dashboard**: https://mr73uh-sfseeurope-ulas-aws-us-west-2.snowflakecomputing.app

## Snowflake Objects

- **Service**: `TRACKMAN_DW.PUBLIC.TRACKMAN_DASHBOARDS`
- **Compute Pool**: `TRACKMAN_POOL`
- **Image Repository**: `TRACKMAN_DW.PUBLIC.TRACKMAN_IMAGES`
- **Image**: `trackman-dashboard:v2`

## Build & Deploy

```bash
# Build image
docker build --platform linux/amd64 -t trackman-dashboard:v2 .

# Tag for Snowflake registry
docker tag trackman-dashboard:v2 sfseeurope-ulas-aws-us-west-2.registry.snowflakecomputing.com/trackman_dw/public/trackman_images/trackman-dashboard:v2

# Push to registry
docker push sfseeurope-ulas-aws-us-west-2.registry.snowflakecomputing.com/trackman_dw/public/trackman_images/trackman-dashboard:v2
```

## Service Specification

```sql
CREATE SERVICE TRACKMAN_DW.PUBLIC.TRACKMAN_DASHBOARDS
IN COMPUTE POOL TRACKMAN_POOL
FROM SPECIFICATION $$
spec:
  containers:
  - name: trackman-app
    image: /trackman_dw/public/trackman_images/trackman-dashboard:v2
    env:
      SNOWFLAKE_WAREHOUSE: COMPUTE_WH
  endpoints:
  - name: ar
    port: 8501
    public: true
  - name: er
    port: 8502
    public: true
$$
QUERY_WAREHOUSE = COMPUTE_WH;
```
