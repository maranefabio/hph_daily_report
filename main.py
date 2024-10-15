import pandas as pd
import date_entry

date = date_entry.date

m = int(date[0])
y = int(date[1])


rel12 = pd.read_csv(
    'C:\\Users\\fabio.rodrigues\\OneDrive - Associação Internacional Habitat para a Humanidade\\Área de Trabalho\Projetos\\relatorios\\rel12.csv', sep = ';', encoding = 'latin-1', engine = 'python')

rel12 = rel12[['id_doador', 'data_captacao', 'criacao_doacao', 'canal', 'campanha_doacao', 'qtd_pagos', 'frequencia_doacao', 'valor']].copy()

rel12['criacao_d'] = rel12['criacao_doacao'].str[:2].astype('int')
rel12['criacao_m'] = rel12['criacao_doacao'].str[3:5].astype('int')
rel12['criacao_y'] = rel12['criacao_doacao'].str[6:10].astype('int')

rel12.loc[rel12['data_captacao'] == '', 'data_captacao'] = '00/00/0000'
rel12['data_captacao'].fillna('00/00/0000', inplace = True)
rel12['captacao_d'] = rel12['data_captacao'].str[:2].astype('int')
rel12['captacao_m'] = rel12['data_captacao'].str[3:5].astype('int')
rel12['captacao_y'] = rel12['data_captacao'].str[6:10].astype('int')

rel12.drop(['criacao_doacao'], axis = 1, inplace = True)

rel12['valor'] = rel12['valor'].str.replace(',', '.')
rel12['valor'] = rel12['valor'].astype('float')

rel12['qtd_pagos'].fillna(0, inplace = True)
rel12['qtd_pagos'] = rel12['qtd_pagos'].astype('int')

novos_f2f_df = rel12.loc[
    (rel12['captacao_m'] == m) & 
    (rel12['captacao_y'] == y) & 
    (rel12['qtd_pagos'] != 0) &
    ((rel12['campanha_doacao'] == 'F2F HABITAT') | 
     (rel12['campanha_doacao'] == 'F2F - Activa') |
     (rel12['campanha_doacao'] == 'F2F Externo: T4C') |
     (rel12['campanha_doacao'] == 'F2F - Interno') | 
     (rel12['campanha_doacao'] == 'F2F HABITAT (One Off)')
    )
    ] 

novos_tlmkt_df = rel12.loc[
    (rel12['criacao_m'] == m) & 
    (rel12['criacao_y'] == y) &
    (rel12['campanha_doacao'].str.contains('TLMK', case = True)
    )
    ] 

novos_web_df = rel12.loc[
    (rel12['criacao_m'] == m) & 
    (rel12['criacao_y'] == y) & 
    (rel12['qtd_pagos'] != 0) & 
    ((rel12['campanha_doacao'] == 'Web - Geral Oficial Site') |
     (rel12['campanha_doacao'] == 'Web - Atualização de pagamento') |
     (rel12['campanha_doacao'].str.contains('Web Agência', case = False))
    )
    ]

novos_md_df = rel12.loc[
    (rel12['criacao_m'] == m) & 
    (rel12['criacao_y'] == y) &
    (rel12['campanha_doacao'].str.contains('MD_', case = True))
    ] 
novos_md_df = novos_md_df.loc[~(novos_md_df['campanha_doacao'].str.contains('RECORRENTE', case = False))]

#################################################

rel12 = pd.read_csv('C:\\Users\\fabio.rodrigues\\OneDrive - Associação Internacional Habitat para a Humanidade\\Área de Trabalho\Projetos\\relatorios\\rel12.csv', sep = ';', encoding = 'latin-1', engine = 'python')
rel83 = pd.read_csv('C:\\Users\\fabio.rodrigues\\OneDrive - Associação Internacional Habitat para a Humanidade\\Área de Trabalho\Projetos\\relatorios\\rel83.csv', sep = ';', encoding = 'latin-1')

rel12_canais = rel12[['id_doador', 'canal']].copy()

rel83 = rel83[['id_doador', 'criacao_doacao', 'ultima_mudanca_status', 'status', 'reativada']].copy()

rel83['mudanca_y'] = rel83['ultima_mudanca_status'].str[:4].astype('int')
rel83['mudanca_m'] = rel83['ultima_mudanca_status'].str[5:7].astype('int')
rel83['mudanca_d'] = rel83['ultima_mudanca_status'].str[8:10].astype('int')

rel83['criacao_y'] = rel83['criacao_doacao'].str[:4].astype('int')
rel83['criacao_m'] = rel83['criacao_doacao'].str[5:7].astype('int')
rel83['criacao_d'] = rel83['criacao_doacao'].str[8:10].astype('int')

rel83['ultima_mudanca_status'] = pd.to_datetime({
    'year': rel83['mudanca_y'],
    'month': rel83['mudanca_m'],
    'day': rel83['mudanca_d']})

rel83['criacao_doacao'] = pd.to_datetime({
    'year': rel83['criacao_y'],
    'month': rel83['criacao_m'],
    'day': rel83['criacao_d']})

rel83.rename(columns = {'ultima_mudanca_status': 'ultima_mudanca'}, inplace = True)

rel83['timedelta'] = (rel83['ultima_mudanca'] - rel83['criacao_doacao']).dt.days

pausas_df = rel83.loc[
    (rel83['mudanca_m'] == m) &
    (rel83['mudanca_y'] == y) &
    (rel83['status'] == 'Pausada')
    ]

inativadas_df = rel83.loc[
    (rel83['mudanca_m'] == m) &
    (rel83['mudanca_y'] == y) &
    (rel83['status'] == 'Inativa')
    ]

bloqueadas_df = rel83.loc[
    (rel83['mudanca_m'] == m) &
    (rel83['mudanca_y'] == y) &
    (rel83['status'] == 'Bloqueada')
    ]

reativadas_df = rel83.loc[
    (rel83['mudanca_m'] == m) &
    (rel83['mudanca_y'] == y) &
    (rel83['reativada'] == 'Reativada')
    ]

cancelamentos = rel83.loc[
    (rel83['mudanca_m'] == m) &
    (rel83['mudanca_y'] == y) &
    (rel83['status'] == 'Cancelada')
    ]

cancelamentos_df = pd.merge(cancelamentos, rel12_canais, on = 'id_doador', how = 'left')
cancelamentos_df.drop_duplicates(subset =['id_doador', 'criacao_doacao'], inplace = True)
cancelamentos_geral = cancelamentos_df.loc[~cancelamentos_df['canal'].str.contains('F2F')]['id_doador'].count()

cancelamentos_clawback_T4C = cancelamentos_df.loc[
    (cancelamentos_df['canal'] != '01 - F2F Interno') &
    (cancelamentos_df['timedelta'] < 90) &
    (cancelamentos_df['criacao_doacao'] > '2023-05-14')
    ]['id_doador'].count()

cancelamentos_clawback_ACTIVA = cancelamentos_df.loc[
    (cancelamentos_df['canal'] != '01 - F2F Interno') &
    (cancelamentos_df['timedelta'] < 90) &
    (cancelamentos_df['criacao_doacao'] < '2023-05-06')
    ]['id_doador'].count()

cancelamentos_interno_novo = cancelamentos_df.loc[
    (cancelamentos_df['canal'] == '01 - F2F Interno') & 
    (cancelamentos_df['criacao_doacao'] >= '2023-07-12')
    ]['id_doador'].count()

cancelamentos_interno_antigo = cancelamentos_df.loc[
    (cancelamentos_df['canal'] == '01 - F2F Interno') & 
    (cancelamentos_df['criacao_doacao'] < '2023-07-12')
    ]['id_doador'].count()

#################################################

rel98 = pd.read_csv('C:\\Users\\fabio.rodrigues\\OneDrive - Associação Internacional Habitat para a Humanidade\\Área de Trabalho\Projetos\\relatorios\\rel98.csv', sep = ';', encoding = 'latin-1')

rel98 = rel98[['id', 'id_doador', 'campanha_origem_doacao', 'campanha_alteracao', 'tipo', 'data_alteracao']].copy()

rel98['alteracao_y'] = rel98['data_alteracao'].str[6:10].astype('int')
rel98['alteracao_m'] = rel98['data_alteracao'].str[3:5].astype('int')
rel98['alteracao_d'] = rel98['data_alteracao'].str[:2].astype('int')

downgrade_df = rel98.loc[
    (rel98['alteracao_m'] == m) &
    (rel98['alteracao_y'] == y) &
    (rel98['campanha_alteracao'].str.contains('down', case = False)) &
    (rel98['tipo'] == 'DOWNGRADE')
    ]

downgrade = downgrade_df['id'].count()

#################################################
report_donations_df = pd.DataFrame(columns=[
    'Data',
    'F2F-Externo: Doações',
    'F2F-Externo: Receita',
    'F2F-Interno: Doações únicas',
    'F2F - Interno: Receita únicas',
    'F2F-Interno: Doações regulares',
    'F2F - Interno: Receita regulares',
    'Telemarketing: Doações únicas',
    'Telemarketing: Receita únicas',
    'Telemarketing: Doações regulares',
    'Telemarketing: Receita regulares',
    'Web - Site: Doações únicas',
    'Web - Site: Receita únicas',
    'Web - Site: Doações regulares',
    'Web - Site: Receita regulares',
    'Web - Atualização: Doações únicas',
    'Web - Atualização: Receita únicas',
    'Web - Atualização: Doações regulares',
    'Web - Atualização: Receita regulares',
    'Web - Agência: Doações únicas',
    'Web - Agência: Receita únicas',
    'Web - Agência: Doações regulares',
    'Web - Agência: Receita regulares',
    'Mala Direta: Doações',
    'Mala Direta: Receita',
])

report_cancellations_df = pd.DataFrame(columns=[
    'Data',
    'Cancelados geral',
    'Downgrade',
    'Pausados',
    'Clawback - T4C',
    'Clawback - Activa',
    'Cancelados - F2F Interno (antigo)',
    'Cancelados - F2f Interno (novo)'
])

report_overview_df = pd.DataFrame(columns=[
    'Data',
    'Reativados',
    'Inativados',
    'Bloqueados'
])



for day in range(1, 32):
    data = f'{day}-{m}-{y}'
    f2f_externo_doacoes = novos_f2f_df.loc[
        (novos_f2f_df['captacao_d'] == day) & 
        ((novos_f2f_df['campanha_doacao'] == 'F2F Externo: T4C') |
        (novos_f2f_df['campanha_doacao'] == 'F2F - Activa'))]['id_doador'].count()
    f2f_externo_receita = novos_f2f_df.loc[
        (novos_f2f_df['captacao_d'] == day) & 
        ((novos_f2f_df['campanha_doacao'] == 'F2F Externo: T4C') |
        (novos_f2f_df['campanha_doacao'] == 'F2F - Activa'))]['valor'].sum()
    f2f_interno_doacoes_unicas = novos_f2f_df.loc[
        (novos_f2f_df['captacao_d'] == day) & 
        (novos_f2f_df['frequencia_doacao'] == '0-Unica') & 
        ((novos_f2f_df['campanha_doacao'] == 'F2F - Interno') |
        (novos_f2f_df['campanha_doacao'] == 'F2F HABITAT (One Off)') |
        (novos_f2f_df['campanha_doacao'] == 'F2F HABITAT'))]['id_doador'].count()
    f2f_interno_receita_unicas = novos_f2f_df.loc[
        (novos_f2f_df['captacao_d'] == day) & 
        (novos_f2f_df['frequencia_doacao'] == '0-Unica') & 
        ((novos_f2f_df['campanha_doacao'] == 'F2F - Interno') |
        (novos_f2f_df['campanha_doacao'] == 'F2F HABITAT (One Off)') |
        (novos_f2f_df['campanha_doacao'] == 'F2F HABITAT'))]['valor'].sum()
    f2f_interno_doacoes_regulares = novos_f2f_df.loc[
        (novos_f2f_df['captacao_d'] == day) & 
        (novos_f2f_df['frequencia_doacao'] == '1-Mensal') & 
        ((novos_f2f_df['campanha_doacao'] == 'F2F - Interno') |
        (novos_f2f_df['campanha_doacao'] == 'F2F HABITAT (One Off)') |
        (novos_f2f_df['campanha_doacao'] == 'F2F HABITAT'))]['id_doador'].count()
    f2f_interno_receita_regulares = novos_f2f_df.loc[
        (novos_f2f_df['captacao_d'] == day) & 
        (novos_f2f_df['frequencia_doacao'] == '1-Mensal') & 
        ((novos_f2f_df['campanha_doacao'] == 'F2F - Interno') |
        (novos_f2f_df['campanha_doacao'] == 'F2F HABITAT (One Off)') |
        (novos_f2f_df['campanha_doacao'] == 'F2F HABITAT'))]['valor'].sum()
    telemarketing_doacoes_unicas = novos_tlmkt_df.loc[
        (novos_tlmkt_df['criacao_d'] == day) & 
        (novos_tlmkt_df['frequencia_doacao'] == '0-Unica')]['id_doador'].count()
    telemarketing_receita_unicas = novos_tlmkt_df.loc[
        (novos_tlmkt_df['criacao_d'] == day) & 
        (novos_tlmkt_df['frequencia_doacao'] == '0-Unica')]['valor'].sum()
    telemarketing_doacoes_regulares = novos_tlmkt_df.loc[
        (novos_tlmkt_df['criacao_d'] == day) & 
        (novos_tlmkt_df['frequencia_doacao'] == '1-Mensal')]['id_doador'].count()
    telemarketing_receita_regulares = novos_tlmkt_df.loc[
        (novos_tlmkt_df['criacao_d'] == day) & 
        (novos_tlmkt_df['frequencia_doacao'] == '1-Mensal')]['valor'].sum()
    web_site_doacoes_unicas = novos_web_df.loc[
        (novos_web_df['criacao_d'] == day) &
        (novos_web_df['campanha_doacao'] == 'Web - Geral Oficial Site') & 
        (novos_web_df['frequencia_doacao'] == '0-Unica')]['id_doador'].count()
    web_site_receita_unicas = novos_web_df.loc[
        (novos_web_df['criacao_d'] == day) &
        (novos_web_df['campanha_doacao'] == 'Web - Geral Oficial Site') & 
        (novos_web_df['frequencia_doacao'] == '0-Unica')]['valor'].sum()
    web_site_doacoes_regulares = novos_web_df.loc[
        (novos_web_df['criacao_d'] == day) &
        (novos_web_df['campanha_doacao'] == 'Web - Geral Oficial Site') & 
        (novos_web_df['frequencia_doacao'] == '1-Mensal')]['id_doador'].count()
    web_site_receita_regulares = novos_web_df.loc[
        (novos_web_df['criacao_d'] == day) &
        (novos_web_df['campanha_doacao'] == 'Web - Geral Oficial Site') & 
        (novos_web_df['frequencia_doacao'] == '1-Mensal')]['valor'].sum()
    web_atualizacao_doacoes_unicas = novos_web_df.loc[
        (novos_web_df['criacao_d'] == day) &
        (novos_web_df['campanha_doacao'] == 'Web - Atualização de pagamento') & 
        (novos_web_df['frequencia_doacao'] == '0-Unica')]['id_doador'].count()
    web_atualizacao_receita_unicas = novos_web_df.loc[
        (novos_web_df['criacao_d'] == day) &
        (novos_web_df['campanha_doacao'] == 'Web - Atualização de pagamento') & 
        (novos_web_df['frequencia_doacao'] == '0-Unica')]['valor'].sum()
    web_atualizacao_doacoes_regulares = novos_web_df.loc[
        (novos_web_df['criacao_d'] == day) &
        (novos_web_df['campanha_doacao'] == 'Web - Atualização de pagamento') & 
        (novos_web_df['frequencia_doacao'] == '1-Mensal')]['id_doador'].count()
    web_atualizacao_receita_regulares = novos_web_df.loc[
        (novos_web_df['criacao_d'] == day) &
        (novos_web_df['campanha_doacao'] == 'Web - Atualização de pagamento') & 
        (novos_web_df['frequencia_doacao'] == '1-Mensal')]['valor'].sum()
    web_agencia_doacoes_unicas = novos_web_df.loc[
        (novos_web_df['criacao_d'] == day) &
        (novos_web_df['campanha_doacao'].str.contains('Web Agência', case=False)) & 
        (novos_web_df['frequencia_doacao'] == '0-Unica')]['id_doador'].count()
    web_agencia_receita_unicas = novos_web_df.loc[
        (novos_web_df['criacao_d'] == day) &
        (novos_web_df['campanha_doacao'].str.contains('Web Agência', case=False)) & 
        (novos_web_df['frequencia_doacao'] == '0-Unica')]['valor'].sum()
    web_agencia_doacoes_regulares = novos_web_df.loc[
        (novos_web_df['criacao_d'] == day) &
        (novos_web_df['campanha_doacao'].str.contains('Web Agência', case=False)) & 
        (novos_web_df['frequencia_doacao'] == '1-Mensal')]['id_doador'].count()
    web_agencia_receita_regulares = novos_web_df.loc[
        (novos_web_df['criacao_d'] == day) &
        (novos_web_df['campanha_doacao'].str.contains('Web Agência', case=False)) & 
        (novos_web_df['frequencia_doacao'] == '1-Mensal')]['valor'].sum()
    md_doacoes = novos_md_df.loc[
        (novos_md_df['criacao_d'] == day)]['id_doador'].count()
    md_receita = novos_md_df.loc[
        novos_md_df['criacao_d'] == day]['valor'].sum()
    
    new_row_donations = [
        data,
        f2f_externo_doacoes,
        f2f_externo_receita,
        f2f_interno_doacoes_unicas,
        f2f_interno_receita_unicas,
        f2f_interno_doacoes_regulares,
        f2f_interno_receita_regulares,
        telemarketing_doacoes_unicas,
        telemarketing_receita_unicas,
        telemarketing_doacoes_regulares,
        telemarketing_receita_regulares,
        web_site_doacoes_unicas,
        web_site_receita_unicas,
        web_site_doacoes_regulares,
        web_site_receita_regulares,
        web_atualizacao_doacoes_unicas,
        web_atualizacao_receita_unicas,
        web_atualizacao_doacoes_regulares,
        web_atualizacao_receita_regulares,
        web_agencia_doacoes_unicas,
        web_agencia_receita_unicas,
        web_agencia_doacoes_regulares,
        web_agencia_receita_regulares,
        md_doacoes,
        md_receita
    ]    
    report_donations_df.loc[len(report_donations_df)] = new_row_donations

    reactivated = reativadas_df.loc[
        reativadas_df['mudanca_d'] == day]['id_doador'].count()
    inactivated = inativadas_df.loc[
        inativadas_df['mudanca_d'] == day]['id_doador'].count()
    blocked = bloqueadas_df.loc[
        bloqueadas_df['mudanca_d'] == day]['id_doador'].count()
    new_row_overview = [
        data,
        reactivated,
        inactivated,
        blocked
    ]
    report_overview_df.loc[len(report_overview_df)] = new_row_overview

    canc_geral = cancelamentos_df.loc[
        (~cancelamentos_df['canal'].str.contains('F2F')) &
        cancelamentos_df['mudanca_d'] == day]['id_doador'].count()
    downgrades = downgrade_df.loc[
        downgrade_df['alteracao_d'] == day]['id'].count()
    pauses = pausas_df.loc[
        pausas_df['mudanca_d'] == day]['id_doador'].count() 
    canc_t4c = cancelamentos_df.loc[
    (cancelamentos_df['canal'] != '01 - F2F Interno') &
    (cancelamentos_df['timedelta'] < 90) &
    (cancelamentos_df['criacao_doacao'] > '2023-05-14') &
    (cancelamentos_df['mudanca_d'] == day)]['id_doador'].count()
    canc_int = cancelamentos_df.loc[
    (cancelamentos_df['canal'] == '01 - F2F Interno') & 
    (cancelamentos_df['criacao_doacao'] >= '2023-07-12') &
    (cancelamentos_df['mudanca_d'] == day)]['id_doador'].count()
    canc_int_old = cancelamentos_df.loc[
    (cancelamentos_df['canal'] == '01 - F2F Interno') & 
    (cancelamentos_df['criacao_doacao'] < '2023-07-12') &
    (cancelamentos_df['mudanca_d'] == day)]['id_doador'].count()
    canc_activa = cancelamentos_df.loc[
    (cancelamentos_df['canal'] != '01 - F2F Interno') &
    (cancelamentos_df['timedelta'] < 90) &
    (cancelamentos_df['criacao_doacao'] < '2023-05-06') &
    (cancelamentos_df['mudanca_d'] == day)]['id_doador'].count()
    new_row_canc = [
        data,
        canc_geral,
        downgrades,
        pauses,
        canc_t4c,
        canc_int,
        canc_int_old,
        canc_activa
    ]
    report_cancellations_df.loc[len(report_cancellations_df)] = new_row_canc
    
    
report_donations_df.to_excel('relatorio_doacoes.xlsx', index=False)
report_overview_df.to_excel('relatorio_overview.xlsx', index=False)
report_cancellations_df.to_excel('relatorio_cancelamentos.xlsx', index=False)
