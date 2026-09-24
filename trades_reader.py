from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
# from pyflink.common.serialization import JsonRowDeserializationSchema
from pyflink.table.types import DataType
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors.file_system import FileSink,Encoder
from pyflink.datastream.connectors.file_system import OnCheckpointRollingPolicy
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.common.watermark_strategy import WatermarkStrategy,TimestampAssigner
from pyflink.common import Duration 
from pyflink.datastream.window import TumblingEventTimeWindows,TumblingProcessingTimeWindows
from datetime import *
from pyflink.common import Duration,Time
from pyflink.common import Time ,Row
from pyflink.datastream.checkpointing_mode import CheckpointingMode 

env = StreamExecutionEnvironment.get_execution_environment()


class TradeTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self,value,record_timestamp):

        parsed=datetime.fromisoformat(value.timestamp)
        return int(parsed.timestamp()*1000)

watermark_strategy= WatermarkStrategy\
        .for_bounded_out_of_orderness(Duration.of_seconds(10))\
        .with_timestamp_assigner(TradeTimestampAssigner())



trade_schema= Types.ROW_NAMED(
    ["symbol","price","timestamp"],
    [Types.STRING(),Types.DOUBLE(),Types.STRING()])

json_format=JsonRowDeserializationSchema.builder().type_info(trade_schema).build()

source = KafkaSource.builder()\
        .set_bootstrap_servers("localhost:9092")\
        .set_topics("trades")\
        .set_value_only_deserializer(json_format)\
        .build()




# json_format=



stream = env.from_source(source,watermark_strategy,"kafka_trades_source")
env.enable_checkpointing(10000,CheckpointingMode.EXACTLY_ONCE )
windowed= stream \
         .key_by(lambda value1: value1.symbol)\
         .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))\
         .reduce(lambda value1,value2: Row(
             symbol=value1.symbol,
             price=value1.price + value2.price,
             timestamp=value1.timestamp
         ))

windowed.map(lambda row: str(row),output_type=Types.STRING()).sink_to(
    FileSink.for_row_format(
        base_path="output/trades_windowed",
        encoder=Encoder.simple_string_encoder()
    )
    .with_rolling_policy(OnCheckpointRollingPolicy.on_checkpoint_rolling_policy())
    .build()
)
# stream.print()


env.execute("trades_reader_job")