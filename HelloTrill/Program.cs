using System;
using System.Collections.Generic;
using System.Reactive.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.StreamProcessing;

namespace HelloTrill
{
    class Program
    {
        static void Main(string[] args)
        {
            /**
             * Generating synthetic data
             */
            var SIZE = 100;                                    // Size of the data set
            var listA = new List<int>();                        // A list for storing the data points
            var listB = new List<int>();                        // Another list for storing the data points
            for (int i = 0; i < SIZE; i++)
            {
                listA.Add(i);                                    // Populate listA with dummy data
                listB.Add(i);                                    // Populate listB with dummy data
            }

            var window_size = 10;
            /**
             * Creating lists created above to Trill streams
             */
            var streamA = listA                                 // Creating first stream from listA 
                    .ToObservable()                             // Convert the data list to an Observable first 
                    .ToTemporalStreamable(e => e, e => e + 1)   // Then convert to Trill temporal stream;
                ;                                               // nth event in the stream has an integer payload 'n'
                                                                // and an interval of [n, n+1)
            
            var streamB = listB                                 // Creating streamB (not using yet) similar to streamA.
                    .ToObservable()
                    .ToTemporalStreamable(e => e, e => e + 1)
                ;
            
            /**
             * Define transformations on the stream(s) 
             */
            var result = streamA
                //.Select(e => e * 3) // Set transformations on the stream.
                //.Where(e => e % 2 == 0)
                //.TumblingWindowLifetime(10, 10)
                //.Sum(e=> e)
                //.Join(streamB, e=> 1, e=> 1, (left, right) => new {left, right})
                .Multicast(s=> s
                    .TumblingWindowLifetime(10, 10)
                    .Sum(e=> e)
                    .Join(s,  e=> 1, e=> 1, (left, right) => new {left, right}))
                ;                                               // In this case, Adding 1 to each payload using Select
          
            var result3 = streamA
                    .Multicast(s=>s
                        .ShiftEventLifetime(1)
                        //.AlterEventLifetime(e => e + 1, 1)
                        .TumblingWindowLifetime(window_size,0)
                        .ShiftEventLifetime(-window_size)
                        //.AlterEventLifetime(e=>e-window_size, window_size)
                        .Average(e=>e)
                        .Join(s, e=>1, e=>1, (left, right)=> (right-left))
                    )
                    .Multicast(s=>s
                        .Select (e=> e*e)
                        .ShiftEventLifetime(1)
                        //.AlterEventLifetime(e => e + 1, 1)
                        .TumblingWindowLifetime(window_size,0)
                        .ShiftEventLifetime(-10)
                        //.AlterEventLifetime(e=>e-window_size, window_size)
                        .Average(e=>e)
                        .Select(e=>Math.Sqrt(e))
                        .Join(s, e=>1, e=>1, (left, right)=>(right/ left))
                    )
                ;
            /**
             * Print out the result
             */
            result3
                .ToStreamEventObservable()                      // Convert back to Observable (of StreamEvents)
                .Where(e => e.IsData)                           // Only pick data events from the stream
                .ForEach(e => { Console.WriteLine(e); })        // Print the events to the console
                ;
        }
    }
}