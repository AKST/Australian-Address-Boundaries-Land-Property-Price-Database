# Introduction

Hi, I see you're exploring the code, hopefully I
can explain a few things to help you find what
you're looking for. If you've got any questions
please don't heistate to ask, you miss every shot
you don't take! You can ask stuff [here](https://github.com/AKST/Aus-Land-Data-ETL/discussions).

## General Project Structure

- `lib/daemon`, this doesn't have much at this stage, but it'll be
  used for processes that are shared between tasks.
- `lib/defaults`, this is mostly configuration for different instances.
- `lib/pipeline`, this has most of the interesting stuff relating to
  specfic data ingestions.
- `lib/tasks`, this is where most of the command line entries are.
  You will find different tasks there to run operations relating
  to this project.
- `lib/tooling`, kind of like pipeline but code I've written to automate
  maintaince of different portions of this project.
- `lib/utilitiy`, just helper code.
