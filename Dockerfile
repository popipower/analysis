FROM public.ecr.aws/lambda/python:3.9

# Copy function code
COPY app.py /var/task

# Install the function's dependencies using file requirements.txt
# from your project folder.

RUN pip install pandas
RUN pip install pandas-ta
RUN pip install yfinance
RUN pip install datetime


# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "app.handler" ]