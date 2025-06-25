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

        static async Task Main(string[] args)
        {
            while (true)
            {
                Console.WriteLine($"\n=== 🌐 New Telemetry Batch @ {DateTime.Now} ===");
                foreach (var kvp in FACTORY_DEVICES)
                {
                    await SendTelemetryAsync(
                        deviceId: kvp.Key,
                        location: kvp.Value.Item1,
                        certPath: kvp.Value.Item2
                    );
                }

                Console.WriteLine("✅ All factories sent | Sleeping for 15 seconds...\n");
                await Task.Delay(15000);
            }
        }

        static async Task SendTelemetryAsync(string deviceId, string location, string certPath)
        {
            try
            {
                // Load certificate & create auth
                X509Certificate2 certificate =
                    new X509Certificate2(certPath, PFX_PASSWORD);
                DeviceAuthenticationWithX509Certificate auth =
                    new DeviceAuthenticationWithX509Certificate(deviceId, certificate);
                DeviceClient deviceClient =
                    DeviceClient.Create(HUB_HOSTNAME, auth, TransportType.Mqtt);

                // Build readings
                List<object> readings = new List<object>();
                double totalPower = 0, totalEnergy = 0;
                foreach (var machine in MACHINES)
                {
                    var data = GenerateMachineData(
                        machineType: machine.Key,
                        basePower: machine.Value.Item1,
                        baseCurrent: machine.Value.Item2
                    );
                    // extract fields via reflection
                    totalPower += (double)data.GetType()
                                               .GetProperty("activePower")
                                               .GetValue(data, null);
                    totalEnergy += (double)data.GetType()
                                               .GetProperty("energyConsumption")
                                               .GetValue(data, null);
                    readings.Add(data);
                }

                // Create payload with explicit timestamp
                string timestamp = DateTime.UtcNow.ToString("o");
                var payload = new
                {
                    timestamp,
                    factoryId = deviceId,
                    location,
                    readings,
                    totalActivePower = Math.Round(totalPower, 1),
                    totalEnergyConsumption = Math.Round(totalEnergy, 1)
                };
                string jsonPayload = JsonConvert.SerializeObject(payload);
                Message message = new Message(Encoding.UTF8.GetBytes(jsonPayload));

                // Send
                await deviceClient.SendEventAsync(message);
                // ⏱ 5-second delay before the next factory send:
                await Task.Delay(5000);
                // 📣 Enhanced console log:
                Console.WriteLine(
                    $"✅ Sent telemetry | Factory: {deviceId} | " +
                    $"Location: {location} | Timestamp: {timestamp}"
                );
            }
            catch (Exception ex)
            {
                Console.WriteLine(
                    $"❌ Error sending data for {deviceId}: {ex.Message}"
                );
            }
        }

        static object GenerateMachineData(string machineType,
                                         double basePower,
                                         double baseCurrent)
        {
            double activePower = Math.Max(0.1, Gaussian(basePower, basePower * 0.1));
            double current = Math.Max(0.1, Gaussian(baseCurrent, baseCurrent * 0.1));

            return new
            {
                machineType,
                voltage = Math.Round(RandomBetween(410, 420), 1),
                current = Math.Round(current, 1),
                activePower = Math.Round(activePower, 1),
                powerFactor = Math.Round(RandomBetween(0.92, 0.98), 2),
                energyConsumption = Math.Round(activePower * RandomBetween(0.8, 1.2), 1),
                status = rnd.NextDouble() > 0.1 ? "Normal" : "Alert"
            };
        }

        static double Gaussian(double mean, double stdDev)
        {
            double u1 = 1.0 - rnd.NextDouble();
            double u2 = 1.0 - rnd.NextDouble();
            double randStdNormal =
                Math.Sqrt(-2.0 * Math.Log(u1)) *
                Math.Sin(2.0 * Math.PI * u2);
            return mean + stdDev * randStdNormal;
        }

        static double RandomBetween(double min, double max)
        {
            return min + rnd.NextDouble() * (max - min);
        }
    }
}
