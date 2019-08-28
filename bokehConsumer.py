from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import pandas as pd
from bokeh.io import curdoc, output_file, show
from bokeh.layouts import row
from bokeh.plotting import figure
from bokeh.client import push_session
import sys

MAX_MSGS = 1000
WINDOW_SIZE = 100
DELTAT = 61

#type `bokeh serve` in terminal to start server
# This is a callback function updates the data source for the plot
def update(consumer, df, df2, df3, df4, df5, df6, df7):
  i=0
  while i < 50:
    i=i+1
    msg = consumer.poll(timeout=1.0)
    if msg is None:
      continue
    if msg.error():
      # Error or event
      if msg.error().code() == KafkaError._PARTITION_EOF:
        # End of partition event
        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                         (msg.topic(), msg.partition(), msg.offset()))
      elif msg.error():
        # Error
        raise KafkaException(msg.error())
    else:
      # Proper message
      sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                       (msg.topic(), msg.partition(), msg.offset(),
                        str(msg.key())))
      print(msg.value())
      # parse the message
      val = msg.value()
      result = json.loads(val)
      #t = datetime.strptime(result['time'], "%H:%M:%S.%f")
      t = float(result['timestamp'])
      vINX = float(result['INX_perChange'])
      vMSFT = float(result['MSFT_perChange'])
      vBA = float(result['BA_perChange'])
      volMSFT = float(result['MSFT_vol'])

      # add to the dataframe
      df.loc[len(df)] = [t, vINX]
      df2.loc[len(df2)] = [t, vMSFT]
      df3.loc[len(df3)] = [t, vBA]
      df4.loc[len(df4)] = [t, vMSFT/vINX]
      df5.loc[len(df5)] = [t, vBA/vINX]
      if len(df6['value'])>0:
        df6.loc[len(df6)] = [t, (vMSFT-df6['value'].iloc[-1])/DELTAT]
        df7.loc[len(df7)] = [t, (volMSFT-df7['value'].iloc[-1])/DELTAT]
        # df6.loc[len(df6)] = 1
        # df7.loc[len(df7)] = 1
      else:
        df6.loc[len(df6)] = 0
        df7.loc[len(df7)] = 0

      # Sliding window of the WINDOW_SIZE most recent values
      if len(df['value']) > WINDOW_SIZE:
        r.data_source.data['y'] = list(df['value'])[-WINDOW_SIZE:]
        r.data_source.data['x'] = range(len(list(df['value'])))[-WINDOW_SIZE:]
        dots.data_source.data['y'] = list(df['value'])[-WINDOW_SIZE:]
        dots.data_source.data['x'] = range(len(list(df['value'])))[-WINDOW_SIZE:]
      else:
        r.data_source.data['y'] = list(df['value'])
        r.data_source.data['x'] = range(len(list(df['value'])))
        dots.data_source.data['y'] = list(df['value'])
        dots.data_source.data['x'] = range(len(list(df['value'])))

      # Sliding window of the WINDOW_SIZE most recent values
      if len(df2['value']) > WINDOW_SIZE:
        r2.data_source.data['y'] = list(df2['value'])[-WINDOW_SIZE:]
        r2.data_source.data['x'] = range(len(list(df2['value'])))[-WINDOW_SIZE:]
        dots2.data_source.data['y'] = list(df2['value'])[-WINDOW_SIZE:]
        dots2.data_source.data['x'] = range(len(list(df2['value'])))[-WINDOW_SIZE:]
      else:
        r2.data_source.data['y'] = list(df2['value'])
        r2.data_source.data['x'] = range(len(list(df2['value'])))
        dots2.data_source.data['y'] = list(df2['value'])
        dots2.data_source.data['x'] = range(len(list(df2['value'])))

      # Sliding window of the WINDOW_SIZE most recent values
      if len(df3['value']) > WINDOW_SIZE:
        r3.data_source.data['y'] = list(df3['value'])[-WINDOW_SIZE:]
        r3.data_source.data['x'] = range(len(list(df3['value'])))[-WINDOW_SIZE:]
        dots3.data_source.data['y'] = list(df3['value'])[-WINDOW_SIZE:]
        dots3.data_source.data['x'] = range(len(list(df3['value'])))[-WINDOW_SIZE:]
      else:
        r3.data_source.data['y'] = list(df3['value'])
        r3.data_source.data['x'] = range(len(list(df3['value'])))
        dots3.data_source.data['y'] = list(df3['value'])
        dots3.data_source.data['x'] = range(len(list(df3['value'])))

      # Sliding window of the WINDOW_SIZE most recent values
      if len(df4['value']) > WINDOW_SIZE:
        r4.data_source.data['y'] = list(df4['value'])[-WINDOW_SIZE:]
        r4.data_source.data['x'] = range(len(list(df4['value'])))[-WINDOW_SIZE:]
        dots4.data_source.data['y'] = list(df4['value'])[-WINDOW_SIZE:]
        dots4.data_source.data['x'] = range(len(list(df4['value'])))[-WINDOW_SIZE:]
      else:
        r4.data_source.data['y'] = list(df4['value'])
        r4.data_source.data['x'] = range(len(list(df4['value'])))
        dots4.data_source.data['y'] = list(df4['value'])
        dots4.data_source.data['x'] = range(len(list(df4['value'])))

      # Sliding window of the WINDOW_SIZE most recent values
      if len(df5['value']) > WINDOW_SIZE:
        r5.data_source.data['y'] = list(df5['value'])[-WINDOW_SIZE:]
        r5.data_source.data['x'] = range(len(list(df5['value'])))[-WINDOW_SIZE:]
        dots5.data_source.data['y'] = list(df5['value'])[-WINDOW_SIZE:]
        dots5.data_source.data['x'] = range(len(list(df5['value'])))[-WINDOW_SIZE:]
      else:
        r5.data_source.data['y'] = list(df5['value'])
        r5.data_source.data['x'] = range(len(list(df5['value'])))
        dots5.data_source.data['y'] = list(df5['value'])
        dots5.data_source.data['x'] = range(len(list(df5['value'])))

      # Sliding window of the WINDOW_SIZE most recent values
      if len(df6['value']) > WINDOW_SIZE:
        r6.data_source.data['y'] = list(df6['value'])[-WINDOW_SIZE:]
        r6.data_source.data['x'] = range(len(list(df6['value'])))[-WINDOW_SIZE:]
        dots6.data_source.data['y'] = list(df6['value'])[-WINDOW_SIZE:]
        dots6.data_source.data['x'] = range(len(list(df6['value'])))[-WINDOW_SIZE:]
      else:
        r6.data_source.data['y'] = list(df6['value'])
        r6.data_source.data['x'] = range(len(list(df6['value'])))
        dots6.data_source.data['y'] = list(df6['value'])
        dots6.data_source.data['x'] = range(len(list(df6['value'])))

      # Sliding window of the WINDOW_SIZE most recent values
      if len(df7['value']) > WINDOW_SIZE:
        r7.data_source.data['y'] = list(df7['value'])[-WINDOW_SIZE:]
        r7.data_source.data['x'] = range(len(list(df7['value'])))[-WINDOW_SIZE:]
        dots7.data_source.data['y'] = list(df7['value'])[-WINDOW_SIZE:]
        dots7.data_source.data['x'] = range(len(list(df7['value'])))[-WINDOW_SIZE:]
      else:
        r7.data_source.data['y'] = list(df7['value'])
        r7.data_source.data['x'] = range(len(list(df7['value'])))
        dots7.data_source.data['y'] = list(df7['value'])
        dots7.data_source.data['x'] = range(len(list(df7['value'])))

# A Kafka consumer listens for messages on the 'wave' topic and plots
# up-to-date results in a Bokeh plot
if __name__ == '__main__':
  #output_file("layout.html")

  # Initiate connection to Kafka (consumer) and Redis
  print("Starting Producer .... ")

  topics = ''.split(",")
  print("topic:" + topics[0])

  conf = {
    'bootstrap.servers': "",
    'group.id': "cf-consumer",
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'},
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'SCRAM-SHA-256',
    'sasl.username': "",
    'sasl.password': ""
  }

  c = Consumer(**conf)
  c.subscribe(topics)

  print("Config:")
  print(conf)
  print("--------")

  # push this plotting session to Bokeh page
  session = push_session(curdoc())

  # dataframe that is updated with all new data
  df = pd.DataFrame(columns=['time', 'value'])
  df2 = pd.DataFrame(columns=['time', 'value'])
  df3 = pd.DataFrame(columns=['time', 'value'])
  df4 = pd.DataFrame(columns=['time', 'value'])
  df5 = pd.DataFrame(columns=['time', 'value'])
  df6 = pd.DataFrame(columns=['time', 'value'])
  df7 = pd.DataFrame(columns=['time', 'value'])

  # data vars
  time, value = [0], [0]

  # figure that is updated with new data
  plot = figure(plot_width=500, plot_height=500, title="Percent Change")
  r = plot.line(time, value, color='navy', legend="SP500 (INX)")
  dots = plot.circle(time, value, size=12, color='navy', alpha=0.5, legend="SP500 (INX)")
  r2 = plot.line(time, value, color='red', legend="Microsoft (MSFT)")
  dots2 = plot.circle(time, value, size=12, color='red', alpha=0.5, legend="Microsoft (MSFT)")
  r3 = plot.line(time, value, color='green', legend="Boeing (BA)")
  dots3 = plot.circle(time, value, size=12, color='green', alpha=0.5, legend="Boeing (BA)")

  plot2 = figure(plot_width=500, plot_height=500, title="Percent Change Normalized by SP500")
  r4 = plot2.line(time, value, color='red', legend="Microsoft (MSFT)")
  dots4 = plot2.circle(time, value, size=12, color='red', alpha=0.5, legend="Microsoft (MSFT)")
  r5 = plot2.line(time, value, color='green', legend="Boeing (BA)")
  dots5 = plot2.circle(time, value, size=12, color='green', alpha=0.5, legend="Boeing (BA)")

  plot3 = figure(plot_width=500, plot_height=500, title="Volume and Price Slope Over Last Two Messages")
  r6 = plot3.line(time, value, color='red', legend="MSFT Vol Slope")
  dots6 = plot3.circle(time, value, size=12, color='red', alpha=0.5, legend="MSFT Vol Slope")
  r7 = plot3.line(time, value, color='firebrick', legend="MSFT Price Slope")
  dots7 = plot3.circle(time, value, size=12, color='firebrick', alpha=0.5, legend="MSFT Price Slope")

  # check for new kafka events every second
  curdoc().add_periodic_callback(lambda: update(c, df, df2, df3, df4, df5, df6, df7), 1000)
  session.show(row(plot,plot2,plot3)) # open the document in a browser
  session.loop_until_closed()  # run forever