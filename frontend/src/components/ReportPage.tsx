import React, { useState } from 'react';
import { useKeycloak } from '@react-keycloak/web';

type Record = {
  date: string
  user_id: string;
  prosthesis_type: string;
  muscle_group: string;
  signals_count: number;
  signal_frequency_avg: number;
  signal_duration_avg: number;
  signal_amplitude_avg: number;
  signal_duration_total: number;
}

const ReportPage: React.FC = () => {
  const { keycloak, initialized } = useKeycloak();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [response, setResponse] = useState<Record[] | null>(null);

  const downloadReport = async () => {
    if (!keycloak?.token) {
      setError('Not authenticated');
      return;
    }

    try {
      setLoading(true);
      setError(null);

      const response = await fetch(`${process.env.REACT_APP_API_URL}/reports`, {
        headers: {
          'Authorization': `Bearer ${keycloak.token}`
        }
      });
      if (!response.ok) {
        setError(`HTTP error! status: ${response.status}`);
        return;
      }

      setResponse(await response.json());

      
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
    } finally {
      setLoading(false);
    }
  };

  if (!initialized) {
    return <div>Loading...</div>;
  }

  if (!keycloak.authenticated) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
        <button
          onClick={() => keycloak.login()}
          className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
        >
          Login
        </button>
      </div>
    );
  }

  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
      <div className="p-8 bg-white rounded-lg shadow-md">
        <h1 className="text-2xl font-bold mb-6">Usage Reports</h1>

        {keycloak.tokenParsed?.realm_access?.roles.includes('prothetic_user') && (<button
          onClick={downloadReport}
          disabled={loading}
          className={`px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600 ${
            loading ? 'opacity-50 cursor-not-allowed' : ''
          }`}
        >
          {loading ? 'Generating Report...' : 'Get Report'}
        </button>) || (
            <button onClick={() => keycloak.logout()}>Logout</button>
        )}
        {response && (
            <table border={1}>
              <tr>
                <td>Date</td>
                <td>User ID</td>
                <td>Prosthesis Type</td>
                <td>Muscle Group</td>
                <td>Signal Frequency</td>
                <td>Signal Duration</td>
                <td>Signal Amplitude</td>
                <td>Signals count</td>
              </tr>
              {response.map((record, index) => (
                  <tr key={index}>
                    <td>{record.date}</td>
                    <td>{record.user_id}</td>
                    <td>{record.prosthesis_type}</td>
                    <td>{record.muscle_group}</td>
                    <td>{record.signal_frequency_avg}</td>
                    <td>{record.signal_duration_avg}</td>
                    <td>{record.signal_amplitude_avg}</td>
                    <td>{record.signals_count}</td>
                  </tr>
                ))}
            </table>
        )}
        {error && (
          <div className="mt-4 p-4 bg-red-100 text-red-700 rounded">
            {error}
          </div>
        )}
      </div>
    </div>
  );
};

export default ReportPage;