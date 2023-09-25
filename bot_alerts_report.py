import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import pandahouse as ph
from datetime import date
from datetime import timedelta
import sys
import os
import io
from datetime import date, datetime, timedelta
from airflow.decorators import dag, task


#Соединение с Clickhouse
connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database': 'simulator_20230820',
    'user': 'student',
    'password': 'dpo_python_2020'
}

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'ayurina',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 9, 15),
}

#Интервал запуска DAG (МСК)
schedule_interval = '0,15,30,45 * * * *' 


query_feed = ''' SELECT toStartOfFifteenMinutes(time) as ts,
                toDate(ts) as date,
                formatDateTime(ts, '%R') as hm,
                uniqExact(user_id) as dau_lenta,
                countIf(user_id, action='view') as views,
                countIf(user_id, action='like')as likes,
                ROUND(countIf(user_id, action='like')/countIf(user_id, action='view'), 3) as CTR
            FROM simulator_20230820.feed_actions
            WHERE date = today()-7 or ((date between today()-1 and today()) and ts < toStartOfFifteenMinutes(now()))
            GROUP BY ts, date, hm
            ORDER BY ts DESC'''

query_message = '''SELECT toStartOfFifteenMinutes(time) as ts,
                uniqExact(user_id) as dau_msg,
                count(time) as count_message
            FROM simulator_20230820.message_actions
            WHERE toDate(ts) = today()-7 or ((toDate(ts) between today()-1 and today()) and ts < toStartOfFifteenMinutes(now()))
            GROUP BY ts
            ORDER BY ts DESC
            '''

def check_anomaly(df, metric):
    #функция предлагает алгоритм поиска аномалий в данных
    threshold=0.25
    
    current_ts = df['ts'].max()  # достаем максимальную 15-минутку из датафрейма - ту, которую будем проверять на аномальность
    day_ago_ts = current_ts - pd.DateOffset(days=1)  # достаем такую же 15-минутку сутки назад
    week_ago_ts = current_ts - pd.DateOffset(days=7)  # достаем такую же 15-минутку 1 неделю назад

    current_value = df[df['ts'] == current_ts][metric].iloc[0] # достаем из датафрейма значение метрики в максимальную 15-минутку
    day_ago_value = df[df['ts'] == day_ago_ts][metric].iloc[0] # достаем из датафрейма значение метрики в такую же 15-минутку сутки назад
    week_ago_value = df[df['ts'] == week_ago_ts][metric].iloc[0] # достаем из датафрейма значение метрики в такую же 15-минутку 1 неделю назад
    
    # вычисляем отклонение
    if current_value <= day_ago_value:
        diff_day = abs(current_value / day_ago_value - 1)
    else:
        diff_day = abs(day_ago_value / current_value - 1)
        
    if current_value <= week_ago_value:
        diff_week = abs(current_value / week_ago_value - 1)
    else:
        diff_week = abs(week_ago_value / current_value - 1)

    # проверяем больше ли отклонение метрики заданного порога threshold, если отклонение больше, то вернем 1, в противном случае 0
    if diff_day > threshold:
        if diff_week > threshold:
            is_alert = 1
        else:
            is_alert = 0
    else:
        is_alert = 0

    return is_alert, current_ts, current_value, diff_day, diff_week 


def make_plot(data, metric):
    sns.set(rc={'figure.figsize': (16, 10)}) # задаем размер графика
    plt.tight_layout()
    # строим линейный график
    ax = sns.lineplot(data=data.sort_values(by=['date', 'hm']), # задаем датафрейм для графика
                      x="hm", y=metric, # указываем названия колонок в датафрейме для x и y
                      hue="date") # задаем "группировку" на графике, чтобы для каждого значения date была своя линия построена

    for ind, label in enumerate(ax.get_xticklabels()): # этот цикл нужен чтобы разрядить подписи координат по оси Х,
        if ind % 15 == 0:
            label.set_visible(True)
        else:
            label.set_visible(False)
        
    ax.set(xlabel='time') # задаем имя оси Х
    ax.set(ylabel=metric) # задаем имя оси У
    ax.set_title('{}'.format(metric)) # задаем заголовок графика
    ax.set(ylim=(0, None)) # задаем лимит для оси У
    plt.tight_layout()  # избегаем наложения меток
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = '{0}.png'.format(metric)
    plt.close()
    return plot_object


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def alerts_ayurina():

    # Получение данных из базы данных
    @task()
    def extract_df(query_feed, query_message, connection):
        data_feed = ph.read_clickhouse(query=query_feed, connection=connection)
        data_msg = ph.read_clickhouse(query=query_message, connection=connection)
        data = pd.merge(data_feed, data_msg, on='ts')
        return data

    @task()
    def run_alerts(data, chat=None):
        # непосредственно сама система алертов
        bot = telegram.Bot(token="bot_id скрыт") # инициализируем бота
        chat_id = "скрыт"
        metric_list = ['dau_lenta', 'dau_msg', 'views', 'likes', 'CTR', 'count_message']

        for metric in metric_list:
            df = data[['ts', 'date', 'hm', metric]].copy()
            is_alert, current_ts, current_value, diff_day, diff_week = check_anomaly(df, metric)
        
            if is_alert == 1: #для тестирования системы можем добавить or True
                msg = '''‼️⚠️ Метрика {} {} ‼️
Текущее значение = {:.2f}.
Отклонение от вчера на {:.2%}. 
Отклонение от значения неделю назад на {:.2%}.

Графики с основными метриками:
https://superset.lab.karpov.courses/superset/dashboard/4271/'''.format(metric, 
                                                 current_ts, current_value,
                                                 diff_day, diff_week)

                plot_object = make_plot(data, metric)
                # отправляем алерт
                bot.sendMessage(chat_id=chat_id, text=msg)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)


    data = extract_df(query_feed, query_message, connection)
    run_alerts(data, chat=None)

alerts_ayurina = alerts_ayurina()
