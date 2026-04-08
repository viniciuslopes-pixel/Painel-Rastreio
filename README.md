# Painel de rastreamento (Streamlit)

Dashboard Streamlit para acompanhamento operacional a partir de dados **Databricks** (tickets Zendesk com campos customizados de rastreio).

## Requisitos

- Python 3.10+
- Acesso ao Databricks SQL (`databricks_host`, `databricks_http_path`, `databricks_token`)

## Configuração

1. Ambiente e dependências:

   ```powershell
   cd caminho\para\painel-rastreamento
   py -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   ```

2. **Credenciais Databricks:** copie `.env.example` para `.env` na raiz do projeto e preencha com valores reais.  
   Opcional: variável `NUVEM_DOTENV_PATH` com caminho absoluto para outro arquivo `.env`.

3. **Config da aplicação:** copie o template e edite com os IDs do seu Zendesk e o `catalog_schema` do Databricks:

   ```powershell
   copy nuvem_envio_rastreio_config.example.json nuvem_envio_rastreio_config.json
   ```

   O arquivo `nuvem_envio_rastreio_config.json` está no `.gitignore` para não subir IDs e filtros internos por engano.

4. Para descobrir IDs de campos no Databricks, use como base `sql_descobrir_campos_rastreio.sql` (ajuste `seu_catalogo.seu_schema`).

5. **Opcional:** JSON de amostra para drill-down nos gráficos — coloque em `amostras/` e defina `amostra_json_path` (ex.: `amostras/amostra.json`). Arquivos `amostras/*.json` não entram no Git.

## Executar

Na raiz do repositório:

```powershell
streamlit run dashboard_nuvem_envio_rastreio.py
```

Abra o endereço que o terminal mostrar (em geral `http://localhost:8501`).

## Exportar CSV (CLI)

```powershell
python nuvem_envio_rastreio.py --start 2026-03-01 --end 2026-03-31 --tab brasil --out export.csv
```

## Publicar no GitHub

- Não commite `.env`, `credenciais/`, `nuvem_envio_rastreio_config.json`, amostras com dados reais nem `.streamlit/secrets.toml`.
- Faça o primeiro commit com arquivos explícitos (evite `git add .` sem revisar).

## Publicar na internet (Streamlit Community Cloud — gratuito)

O Vercel não hospeda Streamlit; o caminho mais simples é a [Streamlit Community Cloud](https://streamlit.io/cloud) ligada ao seu repositório **GitHub**.

1. Envie o código para um repositório GitHub (recomendado **privado** se o JSON de config tiver IDs internos).
2. Acesse [share.streamlit.io](https://share.streamlit.io), entre com GitHub e crie um app:
   - **Repository:** seu repo  
   - **Main file:** `dashboard_nuvem_envio_rastreio.py`  
   - **Branch:** por exemplo `main`
3. Em **Settings → Secrets**, cole um TOML com as credenciais do Databricks (veja modelo em `.streamlit/secrets.example.toml`):
   - `databricks_host`
   - `databricks_http_path`
   - `databricks_token`
4. **Config do painel (JSON), escolha um dos dois:**
   - **Secrets:** adicione `nuvem_config_json` com o **mesmo conteúdo** do seu `nuvem_envio_rastreio_config.json` (pode ser JSON em uma linha ou bloco multilinha em TOML); **ou**
   - **Arquivo no repo:** faça commit de `nuvem_envio_rastreio_config.json` (adequado para repo **privado**). Para forçar o commit apesar do `.gitignore`: `git add -f nuvem_envio_rastreio_config.json` (revise antes para não subir dados que não devem versionar).
5. Depois do primeiro deploy, se os links dos gráficos precisarem de URL absoluta, preencha `dashboard_base_url` no JSON com a URL pública do app (ex.: `https://seu-app.streamlit.app`).

O código já lê credenciais dos Secrets do Streamlit quando não há `.env` no servidor.

## Licença

Defina a licença do repositório nas configurações do GitHub ou adicione um arquivo `LICENSE` se desejar.
