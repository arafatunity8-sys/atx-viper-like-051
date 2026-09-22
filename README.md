# Free Fire Auto Like API

## URL Format
```
GET /{uid}/{region}/{api_key}
```

### Example
```
http://localhost:5001/1312746262/bd/ag
```

### Parameters
- `uid`: Free Fire Player UID (numeric, e.g. `1312746262`)
- `region`: Server region (e.g. `bd`, `ind` / `in`, `br`, `us`, etc.)
- `api_key`: API key for authorization (default: `ag`)

### Optional Query Parameters
- `?random=true`: Use random token selection instead of rotating batch.

### Additional Endpoints
- `GET /` : Health check and API info
- `GET /token_info` : Check available token counts per server
