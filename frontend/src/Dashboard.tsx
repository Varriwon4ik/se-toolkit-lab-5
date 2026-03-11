import { useState, useEffect } from 'react'
import { Bar, Line } from 'react-chartjs-2'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
)

interface ScoreBucket {
  bucket: string
  count: number
}

interface ScoresResponse {
  lab_id: string
  total_submissions: number
  average_score: number
  score_buckets: ScoreBucket[]
}

interface TimelineEntry {
  date: string
  submissions: number
}

interface TimelineResponse {
  lab_id: string
  timeline: TimelineEntry[]
}

interface TaskPassRate {
  task_id: string
  pass_rate: number
  total_submissions: number
  passed_submissions: number
}

interface PassRatesResponse {
  lab_id: string
  pass_rates: TaskPassRate[]
}

interface DashboardData {
  scores: ScoresResponse | null
  timeline: TimelineResponse | null
  passRates: PassRatesResponse | null
}

const LAB_OPTIONS = ['lab-01', 'lab-02', 'lab-03', 'lab-04', 'lab-05']

export default function Dashboard() {
  const [selectedLab, setSelectedLab] = useState<string>('lab-04')
  const [data, setData] = useState<DashboardData>({
    scores: null,
    timeline: null,
    passRates: null,
  })
  const [loading, setLoading] = useState<boolean>(false)
  const [error, setError] = useState<string | null>(null)

  const apiKey = localStorage.getItem('api_key') ?? ''

  useEffect(() => {
    const fetchData = async () => {
      setLoading(true)
      setError(null)

      try {
        const headers = {
          Authorization: `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        }

        const [scoresRes, timelineRes, passRatesRes] = await Promise.all([
          fetch(`/analytics/scores?lab=${selectedLab}`, { headers }),
          fetch(`/analytics/timeline?lab=${selectedLab}`, { headers }),
          fetch(`/analytics/pass-rates?lab=${selectedLab}`, { headers }),
        ])

        if (!scoresRes.ok) throw new Error(`Scores: HTTP ${scoresRes.status}`)
        if (!timelineRes.ok)
          throw new Error(`Timeline: HTTP ${timelineRes.status}`)
        if (!passRatesRes.ok)
          throw new Error(`Pass rates: HTTP ${passRatesRes.status}`)

        const scores: ScoresResponse = await scoresRes.json()
        const timeline: TimelineResponse = await timelineRes.json()
        const passRates: PassRatesResponse = await passRatesRes.json()

        setData({ scores, timeline, passRates })
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Unknown error')
      } finally {
        setLoading(false)
      }
    }

    if (apiKey) {
      fetchData()
    }
  }, [selectedLab, apiKey])

  if (!apiKey) {
    return (
      <div className="dashboard">
        <h2>Dashboard</h2>
        <p>Please connect with your API key to view the dashboard.</p>
      </div>
    )
  }

  const barChartData = data.scores
    ? {
        labels: data.scores.score_buckets.map((b) => b.bucket),
        datasets: [
          {
            label: 'Number of Submissions',
            data: data.scores.score_buckets.map((b) => b.count),
            backgroundColor: 'rgba(54, 162, 235, 0.6)',
            borderColor: 'rgba(54, 162, 235, 1)',
            borderWidth: 1,
          },
        ],
      }
    : null

  const lineChartData = data.timeline
    ? {
        labels: data.timeline.timeline.map((t) => t.date),
        datasets: [
          {
            label: 'Submissions per Day',
            data: data.timeline.timeline.map((t) => t.submissions),
            fill: false,
            backgroundColor: 'rgba(75, 192, 192, 0.6)',
            borderColor: 'rgba(75, 192, 192, 1)',
            tension: 0.1,
          },
        ],
      }
    : null

  return (
    <div className="dashboard">
      <header className="app-header">
        <h1>Dashboard</h1>
        <select
          value={selectedLab}
          onChange={(e) => setSelectedLab(e.target.value)}
        >
          {LAB_OPTIONS.map((lab) => (
            <option key={lab} value={lab}>
              {lab}
            </option>
          ))}
        </select>
      </header>

      {loading && <p>Loading dashboard data...</p>}
      {error && <p className="error">Error: {error}</p>}

      {!loading && !error && (
        <>
          {data.scores && barChartData && (
            <div className="chart-container">
              <h2>Score Distribution</h2>
              <p>
                Total: {data.scores.total_submissions} | Average:{' '}
                {data.scores.average_score.toFixed(1)}
              </p>
              <Bar
                data={barChartData}
                options={{
                  responsive: true,
                  plugins: {
                    title: {
                      display: true,
                      text: 'Score Buckets',
                    },
                  },
                }}
              />
            </div>
          )}

          {data.timeline && lineChartData && (
            <div className="chart-container">
              <h2>Submissions Over Time</h2>
              <Line
                data={lineChartData}
                options={{
                  responsive: true,
                  plugins: {
                    title: {
                      display: true,
                      text: 'Timeline',
                    },
                  },
                }}
              />
            </div>
          )}

          {data.passRates && (
            <div className="chart-container">
              <h2>Pass Rates by Task</h2>
              <table>
                <thead>
                  <tr>
                    <th>Task ID</th>
                    <th>Pass Rate (%)</th>
                    <th>Passed</th>
                    <th>Total</th>
                  </tr>
                </thead>
                <tbody>
                  {data.passRates.pass_rates.map((task) => (
                    <tr key={task.task_id}>
                      <td>{task.task_id}</td>
                      <td>{(task.pass_rate * 100).toFixed(1)}%</td>
                      <td>{task.passed_submissions}</td>
                      <td>{task.total_submissions}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
    </div>
  )
}
