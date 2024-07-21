from flask import Flask, jsonify
from clickhouse_driver import Client

app = Flask(__name__)

client = Client(host='localhost', port=9000, user='default', password='', database='default')


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/api/daily_summary')
def get_daily_summary():
    query = "SELECT date, drive_count, drive_failures FROM daily_summary"
    data = client.execute(query)

    result = [{"date": row[0], "drive_count": row[1], "drive_failures": row[2]} for row in data]

    return jsonify(result)


@app.route('/api/yearly_summary')
def get_yearly_summary():
    query = "SELECT year, drive_count, drive_failures FROM yearly_summary"
    data = client.execute(query)

    result = [{"year": row[0], "drive_count": row[1], "drive_failures": row[2]} for row in data]

    return jsonify(result)


if __name__ == '__main__':
    app.run()
