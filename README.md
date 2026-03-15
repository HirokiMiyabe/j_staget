# j-staget

J-STAGE Search API (`service=3`) の非公式 Python クライアントです。

> **重要なお知らせ（J-STAGE 利用規約）**
>
> このパッケージは、J-STAGE Search API (`service=3`) の **非公式クライアント** です。
> 利用前に、必ず次のドキュメントを読み、内容に同意してください。
>
> - J-STAGE Terms And Policies:
>   https://www.jstage.jst.go.jp/static/pages/TermsAndPolicies/ForIndividuals/-char/ja
> - J-STAGE WebAPI Terms And Policies:
>   https://www.jstage.jst.go.jp/static/pages/WebAPI/-char/ja
> - About J-STAGE Web API:
>   https://www.jstage.jst.go.jp/static/pages/JstageServices/TAB3/-char/ja
>
> このパッケージを利用する場合、上記規約を自身の責任で遵守するものとします。
> 本パッケージの作者は、その利用によって生じたいかなる損害、損失、違反についても責任を負いません。

## インストール

```bash
pip install j_staget
```

## 使い方

### 主な引数

`fetch` 関数では主に次の引数を指定できます。

- `target_word` (`str`, 省略可)
  検索キーワードです。
  省略可能ですが、その場合は `material`、`author`、`affil`、`issn`、`cdjournal` のいずれかを少なくとも 1 つ指定してください。

- `year` (`int`, 省略可, 既定値: `1950`)
  検索開始年です（J-STAGE API の `pubyearfrom`）。
  `0` を指定すると、利用可能なすべての年が検索対象になります。

- `field` (`str`, 省略可, 既定値: `"article"`)
  `target_word` をどこで検索するかを指定します。
  - `"article"`: 論文タイトル
  - `"abst"`: 抄録
  - `"text"`: 論文本文

- `material` (`str`, 省略可, 既定値: `None`)
  雑誌名または刊行物名で絞り込みます。
  複数語をスペース区切りで指定すると AND 検索になります。

- `author` (`str`, 省略可, 既定値: `None`)
  著者名で絞り込みます。
  複数語をスペース区切りで指定すると AND 検索になります。

- `affil` (`str`, 省略可, 既定値: `None`)
  著者所属で絞り込みます。
  複数語をスペース区切りで指定すると AND 検索になります。

- `issn` (`str`, 省略可, 既定値: `None`)
  `印刷版 ISSN (p-ISSN)` または `オンライン ISSN (e-ISSN)` で絞り込みます。
  `p-ISSN` と `e-ISSN` のどちらでも指定できますが、指定できる ISSN は 1 つだけです。
  この引数は完全一致です（例: `1234-5678`）。
  各ジャーナルに割り当てられた ISSN は、J-STAGE 上のジャーナルトップページで確認できます。

- `cdjournal` (`str`, 省略可, 既定値: `None`)
  ジャーナルコード (`cdjournal`) で絞り込みます。
  名称文字列ではなく、より安定した識別子を使いたい場合に便利です。

- `max_records` (`int`, 省略可, 既定値: `20000`)
  取得するレコード数の上限です。
  過剰な API リクエストを防ぐための安全弁として機能します。

- `sleep` (`float`, 省略可, 既定値: `5.0`)
  連続する API リクエストの間に待機する秒数です。
  J-STAGE サーバーへの負荷を避けるため、必要に応じて大きめに設定することをおすすめします。

### 戻り値

`fetch` 関数は、次の属性を持つ `FetchResult` オブジェクトを返します。

#### `df` (Polars `DataFrame`)

- 型: `polars.DataFrame`
- 各行は、J-STAGE Search API が返した 1 件の論文データに対応します。

#### カラムとデータ型

| 列名             | 型          | 説明        |
|------------------|-------------|-------------|
| `author`         | `list[str]` | 著者名のリストです。日本語名が利用できる場合は日本語名が優先されます。 |
| `article_title`  | `str`       | 論文タイトルです。 |
| `material_title` | `str`       | 掲載誌名または刊行物名です。 |
| `cdjournal`      | `str`       | J-STAGE が提供するジャーナルコードです。 |
| `p_issn`         | `str`       | 掲載誌の `印刷版 ISSN` (`prism:issn`) です。 |
| `o_issn`         | `str`       | 掲載誌の `オンライン ISSN` (`prism:eIssn`) です。 |
| `article_link`   | `str`       | J-STAGE 上の論文ページの URL です。 |
| `pubyear`        | `i32`       | 発行年です。 |
| `doi`            | `str`       | 論文の DOI です（利用可能な場合）。 |
| `url_doi`        | `str`       | `https://doi.org/` を付与した DOI の URL です。 |
| `volume`         | `str`       | 巻です。 |
| `cdvols`         | `null`      | J-STAGE 内部で使われる巻識別子です（`null` の場合があります）。 |
| `number`         | `str`       | 号です。 |
| `starting_page`  | `i32`       | 開始ページです。 |
| `ending_page`    | `i32`       | 終了ページです。 |

> **補足**
> - メタデータの有無によっては、一部のカラムに `null` が入ることがあります。
> - `author` カラムはリスト型です。CSV に出力すると文字列として保存されます。
>   リスト構造を保ちたい場合は、JSON または Parquet 形式がおすすめです。

#### `total_results` (`int | None`)

- J-STAGE API が返す検索結果の総件数です（`openSearch:totalResults`）。
- API 応答から総件数を取得できない場合は、取得できた件数が入ることがあります。

## サンプルコード

[Google Colab上のサンプルコード](https://drive.google.com/file/d/1sFR2WXMFezKYSL2WQRIqIZvK47BobxVM/view?usp=sharing)

```python
from j_staget import fetch

res = fetch(
    target_word="機械学習",
    year=1950,
    field="article",
    max_records=5000,
    sleep=2.0,
)

df = res.df
print(df.shape, res.total_results)
print(df.head())
```



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

## クレジット

- 表示情報提供元: [J-STAGE](https://www.jstage.jst.go.jp/browse/-char/ja)
- Powered by: [J-STAGE](https://www.jstage.jst.go.jp/browse/-char/ja)
