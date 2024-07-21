from flask import Flask, jsonify
from flask_cors import CORS
from clickhouse_driver import Client

app = Flask(__name__)
CORS(app)


def create_clickhouse_client():
    """Create and return a new ClickHouse client instance."""
    return Client(host='localhost', port=9000, user='default', password='', database='default')


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/api/daily_summary')
def get_daily_summary():
    client = create_clickhouse_client()
    try:
        query = "SELECT date, drive_count, drive_failures FROM daily_summary"
        data = client.execute(query)
        result = [{"date": row[0], "drive_count": row[1], "drive_failures": row[2]} for row in data]
        return jsonify(result)
    except Exception as e:
        return str(e), 500
    finally:
        client.disconnect()  # Ensure that the connection is closed properly


@app.route('/api/yearly_summary')
def get_yearly_summary():
    client = create_clickhouse_client()
    try:
        query = "SELECT year, drive_count, drive_failures FROM yearly_summary"
        data = client.execute(query)
        result = [{"year": row[0], "drive_count": row[1], "drive_failures": row[2]} for row in data]
        return jsonify(result)
    except Exception as e:
        return str(e), 500
    finally:
        client.disconnect()  # Ensure that the connection is closed properly


if __name__ == '__main__':
    app.run(debug=True)  # Enable debug mode for easier development
