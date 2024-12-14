using System;
using System.Collections.Generic;
using System.Threading;
using MPI;

class DistributedKeyValueStore
{
	static void Main(string[] args)
	{
		// Initialize the MPI environment
		using (new MPI.Environment(ref args))
		{
			var comm = Communicator.world; // Get the communicator
			int rank = comm.Rank;         // Unique ID for each process
			int size = comm.Size;         // Total number of processes

			// Each process will handle a subset of the key-value store
			Dictionary<string, string> localStore = new Dictionary<string, string>();

			if (rank == 0)
			{
				// Master process handles queries and distributes work
				MasterProcess(comm);
			}
			else
			{
				// Worker processes manage their local key-value pairs
				WorkerProcess(comm, localStore);
			}
		}
	}

	static void MasterProcess(Intracommunicator comm)
	{
		Console.WriteLine("Master: Ready to distribute tasks.");

		// Wait for a short period to ensure all workers are ready
		Thread.Sleep(1000);

		while (true)
		{
			// Simulate receiving a query from a user
			Console.WriteLine("\nEnter a query:\n 1- GET key\n 2- PUT key value\n 3- EXIT for closing processes:");
			string input = GetUserInput();

			if (input == null || input.ToUpper() == "EXIT")
			{
				// Termination signal
				SendExitSignal(comm);
				Console.WriteLine("Master: Shutting down.");
				break;
			}

			// Parse the query
			string[] parts = input.Split(' ', StringSplitOptions.RemoveEmptyEntries);
			if (parts.Length < 2 || (parts[0] != "PUT" && parts[0] != "GET"))
			{
				Console.WriteLine("Invalid query. Try again.");
				continue;
			}

			string command = parts[0];
			string key = parts[1];
			string value = parts.Length > 2 ? parts[2] : null;

			// Determine which process should handle the key
			int targetProcess = GetProcessForKey(key, comm.Size);

			if (command == "PUT")
			{
				HandlePutRequest(comm, targetProcess, key, value);
			}
			else if (command == "GET")
			{
				HandleGetRequest(comm, targetProcess, key);
			}
		}
	}

	static void WorkerProcess(Intracommunicator comm, Dictionary<string, string> localStore)
	{
		Console.WriteLine($"Worker {comm.Rank}: Ready to process tasks.");

		while (true)
		{
			// Receive a message from the master
			string message = comm.Receive<string>(0, 0);

			if (message == "EXIT")
			{
				Console.WriteLine($"Worker {comm.Rank}: Shutting down.");
				break;
			}

			// Parse the command
			string[] parts = message.Split(' ', StringSplitOptions.RemoveEmptyEntries);
			string command = parts[0];
			string key = parts[1];
			string value = parts.Length > 2 ? parts[2] : null;

			if (command == "PUT")
			{
				HandlePutCommand(localStore, key, value);
			}
			else if (command == "GET")
			{
				HandleGetCommand(comm, localStore, key);
			}
		}
	}

	static string GetUserInput()
	{
		return Console.ReadLine();
	}

	static void SendExitSignal(Intracommunicator comm)
	{
		for (int i = 1; i < comm.Size; i++)
		{
			comm.Send("EXIT", i, 0);
		}
	}

	static void HandlePutRequest(Intracommunicator comm, int targetProcess, string key, string value)
	{
		comm.Send($"PUT {key} {value}", targetProcess, 0);
		Console.WriteLine($"Master: PUT request for '{key}' sent to process {targetProcess}.");
	}

	static void HandleGetRequest(Intracommunicator comm, int targetProcess, string key)
	{
		comm.Send($"GET {key}", targetProcess, 0);

		// Wait for the response
		string result = comm.Receive<string>(targetProcess, 0);
		Console.WriteLine($"Master: GET result for '{key}' -> {result}");
	}

	static void HandlePutCommand(Dictionary<string, string> localStore, string key, string value)
	{
		localStore[key] = value!;
		Console.WriteLine($"Worker {Communicator.world.Rank}: Stored '{key}' -> '{value}'.");
	}

	static void HandleGetCommand(Intracommunicator comm, Dictionary<string, string> localStore, string key)
	{
		string result = localStore.ContainsKey(key) ? localStore[key] : "NOT FOUND";
		comm.Send(result, 0, 0); // Send the result back to the master
		Console.WriteLine($"Worker {Communicator.world.Rank}: Retrieved '{key}' -> '{result}'.");
	}

	static int GetProcessForKey(string key, int processCount)
	{
		// Use a simple hash function to determine the target process
		int hash = Math.Abs(key.GetHashCode());
		return (hash % (processCount - 1)) + 1; // Exclude master (rank 0)
	}
}
