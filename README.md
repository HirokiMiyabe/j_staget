# j-staget

Python client for J-STAGE Search API (service=3).


> ⚠️ **Important Notice (J-STAGE Terms of Use)**  
>  
> This package is an **unofficial client** for the J-STAGE Search API (service=3).  
> Before using this package, **you must read and agree to** the following documents:
>  
> - J-STAGE 利用規約
>   https://www.jstage.jst.go.jp/static/pages/TermsAndPolicies/ForIndividuals/-char/ja"
> - J-STAGE WebAPI 利用規約:  
>   https://www.jstage.jst.go.jp/static/pages/WebAPI/-char/ja
> - About J-STAGE Web API:  
>   https://www.jstage.jst.go.jp/static/pages/JstageServices/TAB3/-char/ja
>  
> By using this package, **you acknowledge that you are solely responsible for complying with these terms**.  
> The author of this package assumes **no responsibility or liability** for any damages, losses, or violations arising from its use.



## Install
```bash
pip install j_staget
```
## usage 
### Arguments

The `fetch` function accepts the following arguments:

- `target_word` (`str`, required)  
  The keyword to search for.

- `year` (`int`, optional, default: `1950`)  
  The starting publication year for the search (`pubyearfrom` in the J-STAGE API).  
  Set `0` to search all available years.

- `field` (`str`, optional, default: `"article"`)  
  Specifies which part of the paper is searched:
  - `"article"`: search the target word in **article titles**
  - `"abst"`: search the target word in **abstracts**
  - `"text"`: search the target word in the **full text of papers**

- `max_records` (`int`, optional, default: `20000`)  
  Maximum number of records to retrieve.  
  This is a safety limit to prevent excessive API requests.

- `sleep` (`float`, optional, default: `5.0`)  
  Time in seconds to wait between consecutive API requests.  
  Increasing this value is recommended to avoid overloading the J-STAGE servers.


### sample code
```python
from j_staget import fetch

res = fetch(
    target_word="因果",
    year=1950,
    field="article",
    max_records=5000,
    sleep=2.0,
)

df = res.df
print(df.shape, res.total_results)
print(df.head())
```

## cli
```bash
j-staget "因果" --year 1950 --field article --max-records 5000 --out data/out.parquet
```

## Notes
```yaml

---

## GitHub Actions
`.github/workflows/ci.yml`
```yaml
name: ci
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install -U pip
      - run: pip install -e . pytest
      - run: pytest -q

```



## Credits

- Data source: [J-STAGE](https://www.jstage.jst.go.jp/browse/-char/ja)
- Powered by [J-STAGE](https://www.jstage.jst.go.jp/browse/-char/ja)