using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using MPI;

class DynamicMPIVideoProcessing
{
    const int VIDEO_SIZE_ROWS = 20;
    const int VIDEO_SIZE_COLS = 20;
    const int FRAME_ROWS = 4;
    const int FRAME_COLS = 5;
    const int SIMULATED_DELAY_MS = 50;
    const int TAG_TASK_REQUEST = 1;
    const int TAG_TASK_RESPONSE = 2;
    const int TAG_PROCESSED_FRAME = 3;

    static void PrintFrame(int[,] frame)
    {
        for (int i = 0; i < FRAME_ROWS; i++)
        {
            for (int j = 0; j < FRAME_COLS; j++)
            {
                Console.Write($"{frame[i, j],3} ");
            }
            Console.WriteLine();
        }
    }

    static int[,] GenerateVideo(Intracommunicator comm)
    {
        int[,] video = null;
        if (comm.Rank == 0)
        {
            video = new int[VIDEO_SIZE_ROWS, VIDEO_SIZE_COLS];
            var random = new Random();
            for (int i = 0; i < VIDEO_SIZE_ROWS; i++)
            {
                for (int j = 0; j < VIDEO_SIZE_COLS; j++)
                {
                    video[i, j] = random.Next(0, 256);
                }
            }
        }
        return video;
    }

    static List<int[,]> ExtractFrames(int[,] video)
    {
        List<int[,]> frames = new List<int[,]>();
        for (int i = 0; i <= VIDEO_SIZE_ROWS - FRAME_ROWS; i++)
        {
            for (int j = 0; j <= VIDEO_SIZE_COLS - FRAME_COLS; j++)
            {
                var frame = new int[FRAME_ROWS, FRAME_COLS];
                for (int x = 0; x < FRAME_ROWS; x++)
                {
                    for (int y = 0; y < FRAME_COLS; y++)
                    {
                        frame[x, y] = video[i + x, j + y];
                    }
                }
                frames.Add(frame);
            }
        }
        return frames;
    }

    static int[,] ApplyGaussianBlur(int[,] frame)
    {
        int[,] blurredFrame = new int[FRAME_ROWS, FRAME_COLS];
        for (int i = 0; i < FRAME_ROWS; i++)
        {
            for (int j = 0; j < FRAME_COLS; j++)
            {
                int newValue = frame[i, j] / 2 + 10;
                blurredFrame[i, j] = Math.Clamp(newValue, 0, 255);
            }
        }
        Thread.Sleep(SIMULATED_DELAY_MS);
        return blurredFrame;
    }

    static void WriteToFile(int[,] video, List<int[,]> frames)
    {
        using StreamWriter writer = new StreamWriter("video_and_frames.txt");
        writer.WriteLine("Original Video (20x20):");
        for (int i = 0; i < VIDEO_SIZE_ROWS; i++)
        {
            for (int j = 0; j < VIDEO_SIZE_COLS; j++)
            {
                writer.Write($"{video[i, j],3} ");
            }
            writer.WriteLine();
        }
        writer.WriteLine("\nProcessed Frames:");
        for (int idx = 0; idx < frames.Count; idx++)
        {
            writer.WriteLine($"Frame {idx + 1} (4x5):");
            var frame = frames[idx];
            for (int i = 0; i < FRAME_ROWS; i++)
            {
                for (int j = 0; j < FRAME_COLS; j++)
                {
                    writer.Write($"{frame[i, j],3} ");
                }
                writer.WriteLine();
            }
            writer.WriteLine();
        }
    }

    static void Main(string[] args)
    {
        using (new MPI.Environment(ref args))
        {
            Intracommunicator comm = Communicator.world;
            int rank = comm.Rank;
            int size = comm.Size;
            
            Console.WriteLine("{0} worker is here", rank);
            
            comm.Barrier();

            if (rank == 0)
            {
                Stopwatch stopwatch = new Stopwatch();
                stopwatch.Start();
                // Master process
                int[,] video = GenerateVideo(comm);
                List<int[,]> frames = ExtractFrames(video);
                // frames = frames.GetRange(0, 20); // For testing, keep only first 20 frames
                
                Console.WriteLine($"Total frames generated: {frames.Count}");

                List<int[,]> processedFrames = new List<int[,]>(new int[frames.Count][,]);
                Queue<int> taskQueue = new Queue<int>();
                for (int i = 0; i < frames.Count; i++)
                    taskQueue.Enqueue(i);

                int completedWorkers = 0;
                
                

                while (completedWorkers < size - 1)
                {
                    // Process worker requests
                    if (taskQueue.Count > 0)
                    {
                        Status status = comm.Probe(Communicator.anySource, TAG_TASK_REQUEST);
                        int sourceRank = comm.Receive<int>(status.Source, TAG_TASK_REQUEST);
                        Console.WriteLine("Source Rank: " + sourceRank);

                        // Assign next task
                        int taskIndex = taskQueue.Dequeue();
                        comm.Send(taskIndex, sourceRank, TAG_TASK_RESPONSE);
                        comm.Send(frames[taskIndex], sourceRank, TAG_TASK_RESPONSE);
                        Console.WriteLine($"Assigned task {taskIndex} to worker {sourceRank}");
                    }
                    else
                    {
                        // No tasks left, send termination signal
                        Status status = comm.Probe(Communicator.anySource, TAG_TASK_REQUEST);
                        int sourceRank = comm.Receive<int>(status.Source, TAG_TASK_REQUEST);
                        
                        comm.Send(-1, sourceRank, TAG_TASK_RESPONSE);
                        completedWorkers++;
                    }

                    // Collect processed frames
                    while (comm.ImmediateProbe(Communicator.anySource, TAG_PROCESSED_FRAME) != null)
                    {
                        Status processedStatus = comm.Probe(Communicator.anySource, TAG_PROCESSED_FRAME);
                        int sourceRank = processedStatus.Source;

                        int processedIndex = comm.Receive<int>(sourceRank, TAG_PROCESSED_FRAME);
                        int[,] processedFrame = comm.Receive<int[,]>(sourceRank, TAG_PROCESSED_FRAME);
                        processedFrames[processedIndex] = processedFrame;

                        Console.WriteLine($"Received processed frame {processedIndex} from worker {sourceRank}");
                    }
                }

                try
                {
                    WriteToFile(video, processedFrames);
                    stopwatch.Stop();
                    Console.WriteLine($"Total time taken: {stopwatch.ElapsedMilliseconds} ms");
                }
                catch (Exception e)
                {
                    Console.Write(e.Message);
                }
            }
            else
            {
                // Worker process
                while (true)
                {
                    comm.Send(rank, 0, TAG_TASK_REQUEST);
                    Console.WriteLine("{0} worker has sent task request.", rank);

                    int taskIndex = comm.Receive<int>(0, TAG_TASK_RESPONSE);
                    if (taskIndex == -1) // No more tasks
                        break;

                    int[,] frame = comm.Receive<int[,]>(0, TAG_TASK_RESPONSE);

                    // Process the frame
                    int[,] processedFrame = ApplyGaussianBlur(frame);

                    // Send the processed frame back
                    comm.Send(taskIndex, 0, TAG_PROCESSED_FRAME);
                    comm.Send(processedFrame, 0, TAG_PROCESSED_FRAME);
                }
            }
        }
    }
}
