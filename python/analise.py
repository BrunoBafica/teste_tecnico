import os
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")

PROJETOS_PATH = os.path.join(OUTPUTS_DIR, "projetos_tratados.csv")
MUNICIPIOS_PATH = os.path.join(OUTPUTS_DIR, "municipios_tratados.csv")
RELATORIO_FINAL_PATH = os.path.join(OUTPUTS_DIR, "relatorio_final.csv")


def classificar_risco(row):
    percentual_repasse = 0.0
    if pd.notna(row["valor_total"]) and row["valor_total"] != 0:
        percentual_repasse = row["valor_repassado"] / row["valor_total"]

    atrasado = False
    if pd.notna(row["dt_conclusao_prevista"]):
        atrasado = row["dt_conclusao_prevista"] < pd.Timestamp.today().normalize()

    valor_alto = pd.notna(row["valor_total"]) and row["valor_total"] >= 10_000_000

    if (atrasado and percentual_repasse < 0.5) or (atrasado and valor_alto):
        return "Alto"
    if atrasado or percentual_repasse < 0.5 or valor_alto:
        return "Médio"
    return "Baixo"


def main():
    projetos = pd.read_csv(PROJETOS_PATH)
    municipios = pd.read_csv(MUNICIPIOS_PATH)

    for col in ["dt_inicio", "dt_conclusao_prevista", "dt_conclusao_real"]:
        if col in projetos.columns:
            projetos[col] = pd.to_datetime(projetos[col], errors="coerce")

    df = projetos.merge(municipios, on="id_municipio", how="left")

    # =========================
    # Indicador A — Eficiência de entrega por UF
    # =========================
    concluidos = df[df["situacao"] == "Concluído"].copy()
    concluidos["dias_execucao"] = (
        concluidos["dt_conclusao_real"] - concluidos["dt_inicio"]
    ).dt.days

    indicador_a = df.groupby("uf").agg(
        total_projetos_concluidos=("situacao", lambda x: (x == "Concluído").sum()),
        total_projetos_execucao=("situacao", lambda x: (x == "Em execução").sum())
    ).reset_index()

    media_dias = concluidos.groupby("uf", as_index=False)["dias_execucao"].mean()
    media_dias = media_dias.rename(columns={"dias_execucao": "media_dias_execucao"})

    indicador_a = indicador_a.merge(media_dias, on="uf", how="left")

    denominador = (
        indicador_a["total_projetos_concluidos"] +
        indicador_a["total_projetos_execucao"]
    ).replace(0, pd.NA)

    indicador_a["taxa_conclusao"] = (
        indicador_a["total_projetos_concluidos"] / denominador
    ) * 100

    # =========================
    # Indicador B — Concentração de investimento
    # =========================
    investimento_municipio = df.groupby(
        ["nome_municipio", "uf"], as_index=False
    )["valor_total"].sum()

    top10 = investimento_municipio.nlargest(10, "valor_total").copy()

    total_investimento = df["valor_total"].sum()
    investimento_top10 = top10["valor_total"].sum()
    percentual_top10 = (investimento_top10 / total_investimento * 100) if total_investimento else 0

    proporcao_modalidade = df.groupby("modalidade", as_index=False)["valor_total"].sum()
    if total_investimento:
        proporcao_modalidade["percentual"] = (
            proporcao_modalidade["valor_total"] / total_investimento * 100
        )
    else:
        proporcao_modalidade["percentual"] = 0

    resumo_indicador_b = pd.DataFrame({
        "descricao": ["Percentual do valor total concentrado nos 10 municípios com maior investimento"],
        "valor": [percentual_top10]
    })

    # =========================
    # Indicador C — Déficit habitacional vs unidades entregues
    # =========================
    deficit_por_uf = municipios.groupby("uf", as_index=False)["deficit_habitacional"].sum()
    entregas_por_uf = projetos.merge(
        municipios[["id_municipio", "uf"]],
        on="id_municipio",
        how="left"
    ).groupby("uf", as_index=False)["unidades_entregues"].sum()

    indicador_c = deficit_por_uf.merge(entregas_por_uf, on="uf", how="left")
    indicador_c["unidades_entregues"] = indicador_c["unidades_entregues"].fillna(0)

    indicador_c["percentual_cobertura"] = (
        indicador_c["unidades_entregues"] /
        indicador_c["deficit_habitacional"].replace(0, pd.NA)
    ) * 100

    indicador_c = indicador_c[
        ["uf", "unidades_entregues", "deficit_habitacional", "percentual_cobertura"]
    ]

    # =========================
    # Indicador D — Perfil de risco dos projetos
    # =========================
    ativos = df[df["situacao"].isin(["Em execução", "Contratado"])].copy()

    ativos["percentual_repasse"] = (
        ativos["valor_repassado"] / ativos["valor_total"].replace(0, pd.NA)
    ) * 100

    ativos["atrasado"] = ativos["dt_conclusao_prevista"] < pd.Timestamp.today().normalize()
    ativos["risco"] = ativos.apply(classificar_risco, axis=1)

    projetos_risco = ativos[
        [
            "id_projeto",
            "nome_projeto",
            "nome_municipio",
            "uf",
            "programa",
            "modalidade",
            "situacao",
            "valor_total",
            "valor_repassado",
            "percentual_repasse",
            "dt_conclusao_prevista",
            "atrasado",
            "risco"
        ]
    ].copy()

    # =========================
    # EXPORTAÇÕES
    # =========================
    indicador_a.to_csv(os.path.join(OUTPUTS_DIR, "indicador_a_eficiencia_uf.csv"), index=False)
    top10.to_csv(os.path.join(OUTPUTS_DIR, "indicador_b_top10_municipios.csv"), index=False)
    proporcao_modalidade.to_csv(os.path.join(OUTPUTS_DIR, "indicador_b_modalidade.csv"), index=False)
    resumo_indicador_b.to_csv(os.path.join(OUTPUTS_DIR, "indicador_b_resumo.csv"), index=False)
    indicador_c.to_csv(os.path.join(OUTPUTS_DIR, "indicador_c_deficit_vs_entregas.csv"), index=False)
    projetos_risco.to_csv(os.path.join(OUTPUTS_DIR, "indicador_d_risco_projetos.csv"), index=False)

    relatorio_final = pd.concat(
        [
            indicador_a.assign(secao="Indicador A"),
            top10.assign(secao="Indicador B - Top 10 Municípios"),
            proporcao_modalidade.assign(secao="Indicador B - Modalidade"),
            indicador_c.assign(secao="Indicador C"),
            projetos_risco.assign(secao="Indicador D")
        ],
        ignore_index=True,
        sort=False
    )

    relatorio_final.to_csv(RELATORIO_FINAL_PATH, index=False)

    print("Análises concluídas com sucesso.")
    print(f"Arquivo consolidado gerado em: {RELATORIO_FINAL_PATH}")


if __name__ == "__main__":
    main()
