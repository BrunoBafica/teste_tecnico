import os
import re
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DADOS_DIR = os.path.join(BASE_DIR, "dados")
OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")

os.makedirs(OUTPUTS_DIR, exist_ok=True)


def carregar_csv(nome_arquivo: str) -> pd.DataFrame:
    caminho = os.path.join(DADOS_DIR, nome_arquivo)
    df = pd.read_csv(caminho, encoding="latin1")
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
    )
    return df


def log_nulos(nome_tabela: str, df: pd.DataFrame, campos_chave: list[str], logs: list[str]) -> None:
    for campo in campos_chave:
        qtd_nulos = df[campo].isna().sum() if campo in df.columns else 0
        logs.append(f"[{nome_tabela}] Campo-chave '{campo}' com valores nulos: {qtd_nulos}")


def cpf_invalido(cpf: str) -> bool:
    if pd.isna(cpf):
        return True
    return not bool(re.fullmatch(r"\d{3}\.\d{3}\.\d{3}-\d{2}", str(cpf).strip()))


def calcular_faixa_renda(renda: float) -> str | None:
    if pd.isna(renda):
        return None
    if renda <= 2640:
        return "Faixa 1"
    if renda <= 4400:
        return "Faixa 2"
    return "Faixa 3"


def normalizar_faixa(faixa: str) -> str | None:
    if pd.isna(faixa):
        return None

    faixa = str(faixa).strip().lower()

    if "faixa 1" in faixa or "até r$ 2.640" in faixa or "ate r$ 2.640" in faixa:
        return "Faixa 1"
    if "faixa 2" in faixa or "2.641" in faixa or "4.400" in faixa:
        return "Faixa 2"
    if "faixa 3" in faixa or "4.401" in faixa:
        return "Faixa 3"

    return None


def main():
    logs = []

    municipios = carregar_csv("municipios.csv")
    projetos = carregar_csv("projetos.csv")
    beneficiarios = carregar_csv("beneficiarios.csv")
    pagamentos = carregar_csv("pagamentos.csv")

    # =========================
    # Conversão de datas
    # =========================
    colunas_datas_projetos = ["dt_inicio", "dt_conclusao_prevista", "dt_conclusao_real"]
    for col in colunas_datas_projetos:
        if col in projetos.columns:
            projetos[col] = pd.to_datetime(projetos[col], errors="coerce")

    if "dt_pagamento" in pagamentos.columns:
        pagamentos["dt_pagamento"] = pd.to_datetime(pagamentos["dt_pagamento"], errors="coerce")

    # =========================
    # PROJETOS
    # =========================
    projetos["flag_inconsistencia"] = (
        projetos["dt_conclusao_real"].notna() &
        (projetos["situacao"] != "Concluído")
    )

    logs.append(
        "[projetos] Registros com dt_conclusao_real preenchida mas situacao diferente de "
        f"'Concluído': {projetos['flag_inconsistencia'].sum()}"
    )

    if {"valor_repassado", "valor_total"}.issubset(projetos.columns):
        valor_invalido = projetos["valor_repassado"] > projetos["valor_total"]
        qtd_valor_invalido = valor_invalido.sum()

        projetos["flag_inconsistencia_valor"] = valor_invalido.map({
            True: "Valor Inconsistente",
            False: "Valor Consistente"
        })

        projetos.loc[valor_invalido, "valor_repassado"] = projetos.loc[valor_invalido, "valor_total"]

        logs.append(
            "[projetos] Registros com valor_repassado > valor_total ajustados para valor_total: "
            f"{qtd_valor_invalido}"
        )

    # =========================
    # BENEFICIARIOS
    # =========================
    beneficiarios["flag_cpf_invalido"] = beneficiarios["cpf"].apply(cpf_invalido)

    logs.append(
        f"[beneficiarios] CPFs com formato inválido: {beneficiarios['flag_cpf_invalido'].sum()}"
    )

    faixa_original_normalizada = beneficiarios["faixa_renda"].apply(normalizar_faixa)
    faixa_correta = beneficiarios["renda_mensal"].apply(calcular_faixa_renda)

    beneficiarios["flag_faixa_renda_inconsistente"] = faixa_original_normalizada != faixa_correta
    qtd_faixa_corrigida = beneficiarios["flag_faixa_renda_inconsistente"].sum()

    beneficiarios["faixa_renda_original"] = beneficiarios["faixa_renda"]
    beneficiarios["faixa_renda"] = faixa_correta

    logs.append(
        "[beneficiarios] Faixas de renda recalculadas por inconsistência com renda_mensal: "
        f"{qtd_faixa_corrigida}"
    )

    # =========================
    # PAGAMENTOS
    # =========================
    pagamentos = pagamentos.merge(
        projetos[["id_projeto", "dt_inicio"]],
        on="id_projeto",
        how="left"
    )

    if "dt_pagamento" in pagamentos.columns:
        condicao = (
            pagamentos["dt_pagamento"].notna() &
            pagamentos["dt_inicio"].notna() &
            (pagamentos["dt_pagamento"] < pagamentos["dt_inicio"])
        )

        pagamentos["flag_data_invalida"] = condicao.map({
            True: "Data Inválida",
            False: "Data Válida"
        })

        logs.append(
            "[pagamentos] Pagamentos com data anterior ao início do projeto: "
            f"{condicao.sum()}"
        )
    else:
        pagamentos["flag_data_invalida"] = "Data não informada"
        logs.append("[pagamentos] Coluna 'dt_pagamento' não encontrada.")

    # =========================
    # NULOS EM CAMPOS-CHAVE
    # =========================
    log_nulos("municipios", municipios, ["id_municipio", "nome_municipio", "uf"], logs)
    log_nulos("projetos", projetos, ["id_projeto", "id_municipio", "nome_projeto", "situacao"], logs)
    log_nulos("beneficiarios", beneficiarios, ["id_beneficiario", "id_projeto", "cpf", "renda_mensal"], logs)
    log_nulos("pagamentos", pagamentos, ["id_pagamento", "id_projeto", "dt_pagamento", "status_pagamento"], logs)

    # =========================
    # EXPORTAÇÃO
    # =========================
    municipios.to_csv(
        os.path.join(OUTPUTS_DIR, "municipios_tratados.csv"),
        index=False,
        encoding="utf-8"
    )
    projetos.to_csv(
        os.path.join(OUTPUTS_DIR, "projetos_tratados.csv"),
        index=False,
        encoding="utf-8"
    )
    beneficiarios.to_csv(
        os.path.join(OUTPUTS_DIR, "beneficiarios_tratados.csv"),
        index=False,
        encoding="utf-8"
    )
    pagamentos.to_csv(
        os.path.join(OUTPUTS_DIR, "pagamentos_tratados.csv"),
        index=False,
        encoding="utf-8"
    )

    print("=== LOG DE TRATAMENTO ===")
    for item in logs:
        print(item)

    print("\nArquivos tratados gerados com sucesso em /outputs.")


if __name__ == "__main__":
    main()
