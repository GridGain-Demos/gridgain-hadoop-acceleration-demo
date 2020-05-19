# Hadoop Data Lakes Accelerator With GridGain

This project comprises utility scripts and a reference implementation of loaders for data loading from Hadoop to GridGain 
clusters. The usage of the tools is covered in the ["Architect’s Guide for Hadoop Data Lake Acceleration With GridGain"](TBD)

<p align="center">
    <a href="https://www.gridgain.com/docs/latest/integrations/datalake-accelerator/getting-started">
        <img src="https://www.gridgain.com/docs/latest/images/datalake-gs.png" width="600px"/>
    </a>
</p>

The guide introduces you to an architectural solution that integrates Hadoop with GridGain utilizing 
[GridGain’s Data Lake Accelerator](https://www.gridgain.com/docs/latest/integrations/datalake-accelerator/getting-started).
With such an architecture you will continue using Apache Hadoop for high-latency operations (dozens of seconds, minutes, hours)
and batch processing leaving it to GridGain to take care of those operations that require low-latency response time 
(milliseconds, seconds) or real-time processing.
