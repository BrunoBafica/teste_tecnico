import os
import re
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DADOS_DIR = os.path.join(BASE_DIR, "dados")
OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")

os.makedirs(OUTPUTS_DIR, exist_ok=True)


def carregar_csv(nome_arquivo: str) -> pd.DataFrame:
    caminho = os.path.join(DADOS_DIR, nome_arquivo)
    df = pd.read_csv(caminho)
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


def validar_cpf_formato(cpf: str) -> bool:
    if pd.isna(cpf):
        return False
    return bool(re.fullmatch(r"\d{3}\.\d{3}\.\d{3}-\d{2}", str(cpf)))


def calcular_faixa_renda(renda: float) -> str:
    if pd.isna(renda):
        return None
    if renda <= 2640:
        return "Faixa 1"
    if renda <= 4400:
        return "Faixa 2"
    return "Faixa 3"


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
        f"[projetos] Registros com dt_conclusao_real preenchida mas situacao diferente de 'Concluído': "
        f"{projetos['flag_inconsistencia'].sum()}"
    )

    if {"valor_repassado", "valor_total"}.issubset(projetos.columns):
        mask_valor_invalido = projetos["valor_repassado"] > projetos["valor_total"]
        qtd_valor_invalido = mask_valor_invalido.sum()
        projetos.loc[mask_valor_invalido, "valor_repassado"] = projetos.loc[mask_valor_invalido, "valor_total"]
        logs.append(
            f"[projetos] Registros com valor_repassado > valor_total ajustados para valor_total: {qtd_valor_invalido}"
        )

    # =========================
    # BENEFICIARIOS
    # =========================
    beneficiarios["cpf_invalido"] = ~beneficiarios["cpf"].apply(validar_cpf_formato)
    logs.append(
        f"[beneficiarios] CPFs com formato inválido: {beneficiarios['cpf_invalido'].sum()}"
    )

    faixa_original_diferente = beneficiarios["faixa_renda"] != beneficiarios["renda_mensal"].apply(calcular_faixa_renda)
    qtd_faixa_corrigida = faixa_original_diferente.fillna(False).sum()

    beneficiarios["faixa_renda_original"] = beneficiarios["faixa_renda"]
    beneficiarios["faixa_renda"] = beneficiarios["renda_mensal"].apply(calcular_faixa_renda)

    logs.append(
        f"[beneficiarios] Faixas de renda recalculadas por inconsistência com renda_mensal: {qtd_faixa_corrigida}"
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
        pagamentos["flag_data_invalida"] = (
            pagamentos["dt_pagamento"].notna() &
            pagamentos["dt_inicio"].notna() &
            (pagamentos["dt_pagamento"] < pagamentos["dt_inicio"])
        )

        logs.append(
            f"[pagamentos] Pagamentos com data anterior ao início do projeto: "
            f"{pagamentos['flag_data_invalida'].sum()}"
        )
    else:
        pagamentos["flag_data_invalida"] = False
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
    municipios.to_csv(os.path.join(OUTPUTS_DIR, "municipios_tratados.csv"), index=False)
    projetos.to_csv(os.path.join(OUTPUTS_DIR, "projetos_tratados.csv"), index=False)
    beneficiarios.to_csv(os.path.join(OUTPUTS_DIR, "beneficiarios_tratados.csv"), index=False)
    pagamentos.to_csv(os.path.join(OUTPUTS_DIR, "pagamentos_tratados.csv"), index=False)

    print("=== LOG DE TRATAMENTO ===")
    for item in logs:
        print(item)

    print("\nArquivos tratados gerados com sucesso em /outputs.")


if __name__ == "__main__":
    main()