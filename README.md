### [Система алертов (поиск аномалий)](https://github.com/yurina5t/alert_system/blob/main/bot_alerts_report.py)

Система с периодичность каждые 15 минут проверяет ключевые метрики — такие как активные пользователи в ленте / мессенджере, просмотры, лайки, CTR, количество отправленных сообщений.    
При обнаружении аномального значения в чат отправляется алерт — сообщение со следующей информацией: метрика, ее значение, величина отклонения, график, ссылка на дашборд в BI системе. 

__DAG в Airflow__

<img width="1512" alt="Снимок экрана 2023-09-25 в 12 30 13" src="https://github.com/yurina5t/alert_system/assets/93882842/924f08e9-7689-4b8a-8524-d52b84e51be5">

__Дашборд в BI системе__

<img width="1512" alt="dashbord" src="https://github.com/yurina5t/alert_system/assets/93882842/6dd2e64c-e23f-435b-ab80-730682088adb">


__Bot в telegram__

<img width="752" alt="Снимок экрана 2023-09-25 в 12 32 08" src="https://github.com/yurina5t/alert_system/assets/93882842/ce6c1373-b2dd-416f-8bdd-33d49e3252fe">
