using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Devices.Client;
using Newtonsoft.Json;

namespace FactorySimulatorX509
{
    class Program
    {
        private static readonly Dictionary<string, Tuple<string, string>> FACTORY_DEVICES =
            new Dictionary<string, Tuple<string, string>>
        {
            { "chennai_fact",     Tuple.Create("Chennai",     @"C:\Users\kavin\internship_nebeskie\paleeswari\datasimulator\chennai_Fact\devicea\device1.pfx") },
            { "bangalore_fact",   Tuple.Create("Bangalore",   @"C:\Users\kavin\internship_nebeskie\paleeswari\datasimulator\bangalore_Fact\devic1e.pfx") },
            { "kolkata_fact",     Tuple.Create("Kolkata",     @"C:\Users\kavin\internship_nebeskie\paleeswari\datasimulator\kolkata_fact\device1.pfx") },
            { "coimbatore_fact",  Tuple.Create("Coimbatore",  @"C:\Users\kavin\internship_nebeskie\paleeswari\datasimulator\coimbatore_fact\device1.pfx") },
            { "kochi_fact",       Tuple.Create("Kochi",       @"C:\Users\kavin\internship_nebeskie\paleeswari\datasimulator\kochi_fact\device1.pfx") }
        };

        private static readonly Dictionary<string, Tuple<double, double>> MACHINES =
            new Dictionary<string, Tuple<double, double>>
        {
            { "SeedCleaner",   Tuple.Create(5.0, 12.0) },
            { "Dehuller",      Tuple.Create(12.0, 28.0) },
            { "OilExpeller",   Tuple.Create(27.0, 65.0) },
            { "FilterPress",   Tuple.Create(8.0, 19.0) },
            { "FillingMachine",Tuple.Create(4.0, 10.0) },
            { "HVACSystem",    Tuple.Create(18.0, 42.0) }
        };

        private const string HUB_HOSTNAME = "palesswariiot.azure-devices.net";
        private const string PFX_PASSWORD = "device1123";
        private static readonly Random rnd = new Random();

        // Track total energy per factory
        private static readonly Dictionary<string, double> cumulativeEnergyByFactory = new Dictionary<string, double>();

        static async Task Main(string[] args)
        {
            while (true)
            {
                Console.WriteLine($"\n=== 🌐 New Telemetry Batch @ {DateTime.Now} ===");

                foreach (var kvp in FACTORY_DEVICES)
                {
                    await SendTelemetryAsync(kvp.Key, kvp.Value.Item1, kvp.Value.Item2);
                }

                Console.WriteLine("✅ All factories sent | Sleeping for 15 seconds...\n");
                await Task.Delay(15000);
            }
        }

        static async Task SendTelemetryAsync(string deviceId, string location, string certPath)
        {
            try
            {
                // Load certificate and authenticate
                X509Certificate2 certificate = new X509Certificate2(certPath, PFX_PASSWORD);
                DeviceAuthenticationWithX509Certificate auth = new DeviceAuthenticationWithX509Certificate(deviceId, certificate);
                DeviceClient deviceClient = DeviceClient.Create(HUB_HOSTNAME, auth, TransportType.Mqtt);

                List<object> readings = new List<object>();
                double totalPower = 0;
                double intervalEnergy = 0;
                double durationSeconds = 60;
                double durationHours = durationSeconds / 3600.0;

                foreach (var machine in MACHINES)
                {
                    var data = GenerateMachineData(machine.Key, machine.Value.Item1, machine.Value.Item2);

                    double activePower = (double)data.GetType().GetProperty("activePower").GetValue(data, null);
                    totalPower += activePower;
                    intervalEnergy += activePower * durationHours;

                    readings.Add(data);
                }

                if (!cumulativeEnergyByFactory.ContainsKey(deviceId))
                    cumulativeEnergyByFactory[deviceId] = 0;

                cumulativeEnergyByFactory[deviceId] += intervalEnergy;
                double totalEnergy = cumulativeEnergyByFactory[deviceId];

                string timestamp = DateTime.UtcNow.ToString("o");

                var payload = new
                {
                    timestamp = timestamp,
                    factoryId = deviceId,
                    location = location,
                    readings = readings,
                    totalActivePower = Math.Round(totalPower, 1),
                    totalEnergyConsumption = Math.Round(totalEnergy, 2)
                };

                string jsonPayload = JsonConvert.SerializeObject(payload);
                Message message = new Message(Encoding.UTF8.GetBytes(jsonPayload));
                await deviceClient.SendEventAsync(message);
                await Task.Delay(5000);

                Console.WriteLine($"✅ Sent telemetry | Factory: {deviceId} | TotalEnergy: {totalEnergy} kWh | Timestamp: {timestamp}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Error sending data for {deviceId}: {ex.Message}");
            }
        }

        static object GenerateMachineData(string machineType, double basePower, double baseCurrent)
        {
            double activePower = Math.Max(0.1, Gaussian(basePower, basePower * 0.1));
            double current = Math.Max(0.1, Gaussian(baseCurrent, baseCurrent * 0.1));

            bool isAnomaly = false;
            double pf;

            if (rnd.NextDouble() < 0.5)  // 50% chance of anomaly
            {
                pf = RandomBetween(0.6, 0.84);  // anomalous low power factor
                isAnomaly = true;

                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine($"⚠️  Anomaly injected → {machineType}: PowerFactor = {Math.Round(pf, 2)}");
                Console.ResetColor();
            }
            else
            {
                pf = RandomBetween(0.92, 0.98);  // normal range
            }

            return new
            {
                machineType = machineType,
                voltage = Math.Round(RandomBetween(410, 420), 1),
                current = Math.Round(current, 1),
                activePower = Math.Round(activePower, 1),
                powerFactor = Math.Round(pf, 2),
                energyConsumption = 0, // not needed now
                status = isAnomaly ? "Alert" : "Normal"
            };
        }


        static double Gaussian(double mean, double stdDev)
        {
            double u1 = 1.0 - rnd.NextDouble();
            double u2 = 1.0 - rnd.NextDouble();
            double randStdNormal = Math.Sqrt(-2.0 * Math.Log(u1)) * Math.Sin(2.0 * Math.PI * u2);
            return mean + stdDev * randStdNormal;
        }

        static double RandomBetween(double min, double max)
        {
            return min + rnd.NextDouble() * (max - min);
        }
    }
}
