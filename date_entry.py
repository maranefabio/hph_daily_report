import datetime as dt

print('---------------------------------------------')
print(f'#Insira os valores para gerar o relatório')
print('---------------------------------------------')
    
def validation(y, m):
    try:
        date = dt.datetime(int(y), int(m), 1)
    except ValueError:
        return 1

    today_year = dt.datetime.today().year
    today_month = dt.datetime.today().month
    
    delta_year = today_year - date.year

    if delta_year < 0:
        return 1
    else:
        return 0
    
while True:
    date_dict = {
        'y': str(input('Ano: ')),
        'm': str(input('Mês: '))
        }
    print('---------------------------------------------')

    if len(date_dict['y']) == 2:
        date_dict['y'] = '20' + date_dict['y']

    if validation(**date_dict) == 0:
        month = date_dict['m']
        year = date_dict['y']
        print(f'Entrada escolhida: {month}-{year}')
        break
    else:
        print('# Entrada inválida. Tente novamente.')
        print('---------------------------------------------')

date = [month, year]
if __name__ == '__main__':
    ...