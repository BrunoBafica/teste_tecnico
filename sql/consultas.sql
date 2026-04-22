-- Visão geral dos projetos por UF
SELECT
	municipios.uf,
    COUNT(projetos.id_projeto) AS total_projetos,
    SUM(projetos.unidades_previst) as total_unidades_previstas,
    SUM(projetos.unidades_entregu) as total_unidades_entregues,
    ROUND(SUM(projetos.unidades_entregu) *100.0 / SUM(projetos.unidades_previst),2) as total_porcentagem
FROM projetos
JOIN municipios ON projetos.id_municipio = municipios.id_municipio
GROUP BY municipios.uf
ORDER by total_porcentagem DESC

-- Projetos com atraso
SELECT
	projetos.nome_projeto,
    municipios.nome_municipio,
    municipios.uf,
    projetos.dt_conclusao_pre,
    julianday(DATE('now')) - julianday(projetos.dt_conclusao_pre) as dias_atraso,
    projetos.valor_total
From projetos
JOIN municipios on municipios.id_municipio = projetos.id_municipio
WHERE projetos.situacao = 'Em execução' 
and  projetos.dt_conclusao_pre < DATE('now')
ORDER BY dias_atraso DESC

--Repasse financeiro
SELECT
	projetos.nome_projeto,
    municipios.nome_municipio,
    municipios.uf,
	projetos.valor_total,
    projetos.valor_repassado,
	(projetos.valor_total - projetos.valor_repassado) as valor_pendente,
	ROUND(projetos.valor_repassado *100.0 / projetos.valor_total ,2) AS valor_percentual
FROM projetos
JOIN municipios ON projetos.id_municipio = municipios.id_municipio
WHERE projetos.situacao NOT IN ('Cancelado','Em análise')
AND valor_percentual < 50
ORDER BY valor_percentual DESC

-- Perfil dos beneficiários por programa
SELECT
	projetos.programa,
    COUNT (beneficiarios.id_beneficiario) as total_beneficiario,
    ROUND (SUM(CASE WHEN beneficiarios.sexo='F' THEN 1 ELSE 0 END) * 100.0 / COUNT (*),2) AS percent_fem,
    ROUND (SUM(CASE WHEN projetos.modalidade = 'Faixa 1' THEN 1 else 0 end) * 100.0 / COUNT (*),2) AS percent_benf,
    AVG (beneficiarios.renda_mensal * 1.0) AS renda_media
FROM beneficiarios
JOIN projetos on projetos.id_projeto = beneficiarios.id_projeto
GROUP BY projetos.programa

--Análise de pagamentos por etapa
CREATE VIEW vw_pagamentos_por_etapa AS
SELECT
	pagamentos.etapa,
    COUNT(*) AS total_pagamentos_pago,
	SUM (pagamentos.valor_pago) AS soma_valores_pg,
    AVG (pagamentos.valor_pago) as ticket_med
FROM pagamentos
WHERE pagamentos.status_pagamento = 'Pago'
GROUP by pagamentos.etapa