using System;
using System.Collections.Generic;
using System.IO.MemoryMappedFiles;
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
            for (int i = 0; i < 5; i++)
            {
                listA.Add(i);                                    // Populate listA with dummy data
            }
            for (int i = 5; i < 10; i++)
            {
                listA.Add(i);                                    // Populate listA with dummy data
            }

            for (int i = 10; i < 20; i++)
            {
                listA.Add(i);                                    // Populate listA with dummy data
            }

            double freq1 = 0.5;
            double freq2 = 0.2;
            
            int period1 = Convert.ToInt32( 1/freq1);
            int period2 = Convert.ToInt32( 1/freq2);

            int offset = 0;
            
            for (int i = 0; i < 10; i++)
            {
                listB.Add(i*period1+offset);
            }
            
            

            var window_size = 10;
            /**
             * Creating lists created above to Trill streams
             */
            var streamA = listA                                 // Creating first stream from listA 
                    .ToObservable()                             // Convert the data list to an Observable first 
                    .ToTemporalStreamable(e => e, e => e+1)   // Then convert to Trill temporal stream;
                ;                                               // nth event in the stream has an integer payload 'n'
                                                                // and an interval of [n, n+1)
            
            var streamB = listB                                 // Creating streamB (not using yet) similar to streamA.
                    .ToObservable()
                    .ToTemporalStreamable(e => e, e => e+2)
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
          
            var normalization = streamA
                    .ShiftEventLifetime(1)
                    .TumblingWindowLifetime(window_size,0)
                        /*
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
                        .ShiftEventLifetime(-window_size)
                        //.AlterEventLifetime(e=>e-window_size, window_size)
                        .Average(e=>e)
                        .Select(e=>Math.Sqrt(e))
                        .Join(s, e=>1, e=>1, (left, right)=>(right/ left))
                    )*/
                ;
            var rollingmean = streamA
                    .HoppingWindowLifetime(10, 1)
                    .Average(e=>e)
                ;

            var clipevent = streamA
                .AlterEventLifetime(e => e, StreamEvent.InfinitySyncTime)
                .Multicast(s => s
                    .ClipEventDuration(s, e => 1, e => 1))
                .Join(streamB, e=>1, e=>1, (left, right)=>left)
                ;
            
            var dummyevents = clipevent
                .Chop(0,1)
                .Select((val, e) => val)
                ;

            var gapsize = 3;
            var fillgaps2 = streamA
                .AlterEventLifetime(e => e, gapsize)
                .Multicast(s => s
                    .ClipEventDuration(s, e => 1, e => 1))
                .AlterEventLifetime(start => start, (start, end) => (end - start == gapsize) ? 1 : end - start)
                .Chop(0, 1)
                .Select((val, e) => val)
                ;

            //Similar to the previous exercise, fill the gaps smaller than a given gap tolerance length in the stream.
            //However, every dummy event you use to fill the gap should have a value same as the mean of the signal
            //values that fall within the last a W sized window from the point.
            var windowsize = 5;
            var fillgapswithmean = streamA
                    .AlterEventLifetime(e => e, gapsize)
                    .Multicast(s => s
                        .ClipEventDuration(s, e => 1, e => 1))
                    .Chop(0, 1)
                    .Multicast(s=>
                        s.HoppingWindowLifetime(windowsize,1)
                            .Average(e=>e)
                            .Join(s, l=>1, (r)=> 1, (left, right)=> new{left, right}))
                    .Select((ts, val)=> (ts==val.right)? val.right: val.left)
                ;
            
            
            var sampling = streamB
            .Multicast(s => s
                .AlterEventLifetime(e => e + period1, 1)
                .Join(s, e => 1, e => 1, (left, right) => new {left, right}))
                 
            .AlterEventLifetime(e => e - period1, period1)
               
            .Chop(offset, period2)
               
            .Select((ts, val) => val.left + (ts % period1) * (val.right - val.left) / period1)
            .AlterEventDuration(1)
            .TumblingWindowLifetime(period2, offset)
                /*
                .Multicast(s=>
                     s.Select((ts, val)=>((ts-offset)%period2==0) ? 1:0)
                     .Where(val=>val ==1)
                     .Join(s, e=>1, e=>1, (left, right)=> right))  */
            ;
            
            sampling
                    
                .ToStreamEventObservable()                      // Convert back to Observable (of StreamEvents)
                .Where(e => e.IsData)                           // Only pick data events from the stream
                .ForEach(e => { Console.WriteLine(e); })        // Print the events to the console
                ;
            
           
        }
    }
}