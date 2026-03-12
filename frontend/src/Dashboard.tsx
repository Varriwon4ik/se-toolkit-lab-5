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
  Legend
)

const STORAGE_KEY = 'api_key'

interface ScoreBucket {
  bucket: string
  count: number
}

interface PassRate {
  task: string
  avg_score: number
  attempts: number
}

interface TimelineEntry {
  date: string
  submissions: number
}

interface GroupPerformance {
  group: string
  avg_score: number
  students: number
}

interface DashboardData {
  scores: ScoreBucket[]
  timeline: TimelineEntry[]
  passRates: PassRate[]
  groups: GroupPerformance[]
}

interface DashboardProps {
  selectedLab: string
  onLabChange: (lab: string) => void
}

const LABS = ['lab-01', 'lab-02', 'lab-03', 'lab-04', 'lab-05']

export default function Dashboard({ selectedLab, onLabChange }: DashboardProps) {
  const [data, setData] = useState<DashboardData | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const fetchData = async () => {
      setLoading(true)
      setError(null)

      const token = localStorage.getItem(STORAGE_KEY)
      if (!token) {
        setError('No API token found')
        setLoading(false)
        return
      }

      const headers = { Authorization: `Bearer ${token}` }

      try {
        const [scoresRes, timelineRes, passRatesRes, groupsRes] = await Promise.all([
          fetch(`/analytics/scores?lab=${selectedLab}`, { headers }),
          fetch(`/analytics/timeline?lab=${selectedLab}`, { headers }),
          fetch(`/analytics/pass-rates?lab=${selectedLab}`, { headers }),
          fetch(`/analytics/groups?lab=${selectedLab}`, { headers }),
        ])

        if (!scoresRes.ok) throw new Error(`Scores: HTTP ${scoresRes.status}`)
        if (!timelineRes.ok) throw new Error(`Timeline: HTTP ${timelineRes.status}`)
        if (!passRatesRes.ok) throw new Error(`Pass rates: HTTP ${passRatesRes.status}`)
        if (!groupsRes.ok) throw new Error(`Groups: HTTP ${groupsRes.status}`)

        const scores: ScoreBucket[] = await scoresRes.json()
        const timeline: TimelineEntry[] = await timelineRes.json()
        const passRates: PassRate[] = await passRatesRes.json()
        const groups: GroupPerformance[] = await groupsRes.json()

        setData({ scores, timeline, passRates, groups })
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Unknown error')
      } finally {
        setLoading(false)
      }
    }

    fetchData()
  }, [selectedLab])

  if (loading) {
    return <div className="dashboard-loading">Loading dashboard...</div>
  }

  if (error) {
    return <div className="dashboard-error">Error: {error}</div>
  }

  if (!data) {
    return <div className="dashboard-empty">No data available</div>
  }

  // Bar chart data for score distribution
  const barChartData = {
    labels: data.scores.map((s) => s.bucket),
    datasets: [
      {
        label: 'Number of Students',
        data: data.scores.map((s) => s.count),
        backgroundColor: 'rgba(54, 162, 235, 0.6)',
        borderColor: 'rgba(54, 162, 235, 1)',
        borderWidth: 1,
      },
    ],
  }

  const barChartOptions = {
    responsive: true,
    plugins: {
      legend: {
        position: 'top' as const,
      },
      title: {
        display: true,
        text: 'Score Distribution',
      },
    },
  }

  // Line chart data for submissions over time
  const lineChartData = {
    labels: data.timeline.map((t) => t.date),
    datasets: [
      {
        label: 'Submissions',
        data: data.timeline.map((t) => t.submissions),
        backgroundColor: 'rgba(75, 192, 192, 0.6)',
        borderColor: 'rgba(75, 192, 192, 1)',
        borderWidth: 2,
        tension: 0.3,
      },
    ],
  }

  const lineChartOptions = {
    responsive: true,
    plugins: {
      legend: {
        position: 'top' as const,
      },
      title: {
        display: true,
        text: 'Submissions Over Time',
      },
    },
  }

  return (
    <div className="dashboard">
      <div className="dashboard-header">
        <h1>Dashboard</h1>
        <select
          value={selectedLab}
          onChange={(e) => onLabChange(e.target.value)}
          className="lab-selector"
        >
          {LABS.map((lab) => (
            <option key={lab} value={lab}>
              {lab}
            </option>
          ))}
        </select>
      </div>

      <div className="charts-container">
        <div className="chart-wrapper">
          <Bar data={barChartData} options={barChartOptions} />
        </div>

        <div className="chart-wrapper">
          <Line data={lineChartData} options={lineChartOptions} />
        </div>
      </div>

      <div className="tables-container">
        <h2>Pass Rates by Task</h2>
        <table>
          <thead>
            <tr>
              <th>Task</th>
              <th>Avg Score</th>
              <th>Attempts</th>
            </tr>
          </thead>
          <tbody>
            {data.passRates.map((pr) => (
              <tr key={pr.task}>
                <td>{pr.task}</td>
                <td>{pr.avg_score}</td>
                <td>{pr.attempts}</td>
              </tr>
            ))}
          </tbody>
        </table>

        <h2>Group Performance</h2>
        <table>
          <thead>
            <tr>
              <th>Group</th>
              <th>Avg Score</th>
              <th>Students</th>
            </tr>
          </thead>
          <tbody>
            {data.groups.map((g) => (
              <tr key={g.group}>
                <td>{g.group}</td>
                <td>{g.avg_score}</td>
                <td>{g.students}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
