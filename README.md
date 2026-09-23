# Free Fire Auto Like API

## URL Format
```
GET /{uid}/{region}/{key}
```

### Examples
```
http://localhost:5001/1312746262/bd/ag
http://localhost:5001/1312746262/ind/ag
```

### Parameters
- `uid`: Free Fire Player UID (numeric, e.g. `1312746262`)
- `region`: Server region:
  - `bd` -> loads tokens from `token_bd.json`
  - `ind` / `in` -> loads tokens from `token_ind.json`
  - `{region}` -> dynamically loads tokens from `token_{region}.json`
- `key`: API key for authorization (default: `ag` or keys listed in `api_keys.json`)

### Optional Query Parameters
- `?random=true`: Use random token selection instead of rotating batch.

### Additional Endpoints
- `GET /` : Health check and API info
- `GET /token_info` : Check available token files and counts
