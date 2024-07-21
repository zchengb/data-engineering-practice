import React, { useEffect, useState } from 'react';
import './App.css';
import ReactECharts from 'echarts-for-react';
import axios from 'axios';

function App() {
    const [dailyData, setDailyData] = useState([]);
    const [yearlyData, setYearlyData] = useState([]);

    useEffect(() => {
        // Fetch daily summary data
        axios.get('http://127.0.0.1:5000/api/daily_summary')
            .then(response => {
                const data = response.data;
                setDailyData(data);
            })
            .catch(error => {
                console.error('Error fetching daily summary data:', error);
            });

        // Fetch yearly summary data
        axios.get('http://127.0.0.1:5000/api/yearly_summary')
            .then(response => {
                const data = response.data;
                setYearlyData(data);
            })
            .catch(error => {
                console.error('Error fetching yearly summary data:', error);
            });
    }, []);

    const dailyChartOptions = {
        title: {
            text: 'Daily Drive Metrics'
        },
        tooltip: {
            trigger: 'axis'
        },
        legend: {
            data: ['Drive Count', 'Drive Failures']
        },
        xAxis: [
            {
                type: 'category',
                boundaryGap: false,
                data: dailyData.map(item => item.date)
            }
        ],
        yAxis: [
            {
                type: 'value',
                name: 'Drive Count',
                position: 'left',
                axisLine: { lineStyle: { color: '#3398DB' } },
                axisLabel: { formatter: '{value}' }
            },
            {
                type: 'value',
                name: 'Drive Failures',
                position: 'right',
                axisLine: { lineStyle: { color: '#FF6347' } },
                axisLabel: { formatter: '{value}' }
            }
        ],
        series: [
            {
                name: 'Drive Count',
                type: 'line',
                data: dailyData.map(item => item.drive_count),
                smooth: true,
                yAxisIndex: 0, // Corresponds to the left Y-axis
                itemStyle: {
                    color: '#3398DB'
                },
                areaStyle: {
                    color: 'rgba(51, 152, 219, 0.2)' // Light blue area
                }
            },
            {
                name: 'Drive Failures',
                type: 'line',
                data: dailyData.map(item => item.drive_failures),
                smooth: true,
                yAxisIndex: 1, // Corresponds to the right Y-axis
                itemStyle: {
                    color: '#FF6347'
                },
                areaStyle: {
                    color: 'rgba(255, 99, 71, 0.2)' // Light red area
                }
            }
        ]
    };

    const yearlyChartOptions = {
        title: {
            text: 'Yearly Drive Metrics'
        },
        tooltip: {
            trigger: 'axis'
        },
        legend: {
            data: ['Drive Count', 'Drive Failures']
        },
        xAxis: [
            {
                type: 'category',
                boundaryGap: false,
                data: yearlyData.map(item => item.year)
            }
        ],
        yAxis: [
            {
                type: 'value',
                name: 'Drive Count',
                position: 'left',
                axisLine: { lineStyle: { color: '#3398DB' } },
                axisLabel: { formatter: '{value}' }
            },
            {
                type: 'value',
                name: 'Drive Failures',
                position: 'right',
                axisLine: { lineStyle: { color: '#FF6347' } },
                axisLabel: { formatter: '{value}' }
            }
        ],
        series: [
            {
                name: 'Drive Count',
                type: 'line',
                data: yearlyData.map(item => item.drive_count),
                smooth: true,
                yAxisIndex: 0, // Corresponds to the left Y-axis
                itemStyle: {
                    color: '#3398DB'
                },
                areaStyle: {
                    color: 'rgba(51, 152, 219, 0.2)' // Light blue area
                }
            },
            {
                name: 'Drive Failures',
                type: 'line',
                data: yearlyData.map(item => item.drive_failures),
                smooth: true,
                yAxisIndex: 1, // Corresponds to the right Y-axis
                itemStyle: {
                    color: '#FF6347'
                },
                areaStyle: {
                    color: 'rgba(255, 99, 71, 0.2)' // Light red area
                }
            }
        ]
    };

    return (
        <div className="App">
            <div style={{ width: '80%', height: '400px', margin: '0 auto' }}>
                <ReactECharts option={dailyChartOptions} />
            </div>
            <div style={{ width: '80%', height: '400px', margin: '0 auto' }}>
                <ReactECharts option={yearlyChartOptions} />
            </div>
        </div>
    );
}

export default App;
