from datetime import datetime
import pymysql
import tensorflow as tf
import numpy as np
import configparser


# configuration
#                               O * W + b -> 3 labels for each time series, O[? 7], W[7 3], B[3]
#                               ^ (O: output 7 vec from 7 vec input)
#                               |
#              +-+  +-+        +--+
#              |1|->|2|-> ...  |60| time_step_size = 60
#              +-+  +-+        +--+
#               ^    ^    ...   ^
#               |    |          |
# time series1:[7]  [7]   ...  [7]
# time series2:[7]  [7]   ...  [7]
# time series3:[7]  [7]   ...  [7]
# ...
# time series(250) or time series(750) (batch_size 250 or test_size 1000 - 250)
#      each input size = input_vec_size=lstm_size=7

# configuration variables
class TFStockStudy:
    def __init__(self):
        self.input_vec_size = 7
        self.lstm_size = 7
        self.time_step_size = 60
        self.label_size = 3
        self.evaluate_size = 3
        self.lstm_depth = 4
        self.total_size = 60000
        self.batch_size = 15000
        self.test_size = self.total_size - self.batch_size
        cf = configparser.ConfigParser()
        cf.read('config/config.cfg')
        self.connection = pymysql.connect(host=cf.get('db', 'DB_IP'),
                                          user=cf.get('db', 'DB_USER'),
                                          password=cf.get('db', 'DB_PWD'),
                                          db=cf.get('db', 'DB_SCH'),
                                          charset='utf8mb4',
                                          cursorclass=pymysql.cursors.DictCursor)

    def init_weights(self, shape):
        return tf.Variable(tf.random_normal(shape, stddev=0.01))

    def model(self, X, W, B, lstm_size):
        # X, input shape: (batch_size, time_step_size, input_vec_size)
        XT = tf.transpose(X, [1, 0, 2])  # permute time_step_size and batch_size
        # XT shape: (time_step_size, batch_size, input_vec_size)
        XR = tf.reshape(XT, [-1, lstm_size])  # each row has input for each lstm cell (lstm_size=input_vec_size)
        # XR shape: (time_step_size * batch_size, input_vec_size)
        X_split = tf.split(0, self.time_step_size, XR)  # split them to time_step_size (60 arrays)
        # Each array shape: (batch_size, input_vec_size)
        # Make lstm with lstm_size (each input vector size)
        cell = tf.nn.rnn_cell.GRUCell(lstm_size)
        cell = tf.nn.rnn_cell.DropoutWrapper(cell=cell, output_keep_prob=0.5)
        cell = tf.nn.rnn_cell.MultiRNNCell([cell] * self.lstm_depth, state_is_tuple=True)
    # Get lstm cell output, time_step_size (60) arrays with lstm_size output: (batch_size, lstm_size)
        outputs, _states = tf.nn.rnn(cell, X_split, dtype=tf.float32)
    # Linear activation
    # Get the last output
        return tf.matmul(outputs[-1], W) + B, cell.state_size  # State size to initialize the stat


    def read_series_datas(self, code_dates):
        X = list()
        Y = list()
        for code_date in code_dates:
            cursor = self.connection.cursor()
            cursor.execute(
                "SELECT open, high, low, close, volume, hold_foreign, st_purchase_inst "
                "FROM data.daily_stock WHERE code = %s AND date >= %s ORDER BY date LIMIT %s",
                (code_date[0], code_date[1], self.time_step_size + self.evaluate_size))
            items = cursor.fetchall()

            X.append(np.array(items[:self.time_step_size]))

            price = items[-(self.evaluate_size + 1)][3]
            max = items[-self.evaluate_size][1]
            min = items[-self.evaluate_size][2]

            for item in items[-self.evaluate_size + 1:]:
                if max < item[1]:
                    max = item[1]
                if item[2] < min:
                    min = item[2]

            if (min - price) / price < -0.02:
                Y.append((0., 0., 1.))
            elif (max - price) / price > 0.04:
                Y.append((1., 0., 0.))
            else:
                Y.append((0., 1., 0.))

        arrX = np.array(X)
        norX = (arrX - np.mean(arrX, axis=0)) / np.std(arrX, axis=0)
        return norX, np.array(Y)


    def read_datas(self):
        cursor = self.connection.cursor()
        cursor.execute("SELECT DISTINCT code FROM stock_daily_series")
        codes = cursor.fetchall()
        cursor.execute("SELECT DISTINCT date FROM stock_daily_series ORDER BY date")
        dates = cursor.fetchall()[:-(self.time_step_size + self.evaluate_size)]

        cnt = self.total_size
        code_dates = list()
        for date in dates:
            for code in codes:
                code_dates.append((code[0], date[0]))
                if --cnt <= 0:
                    break
            if --cnt <= 0:
                break

        np.random.seed()
        np.random.shuffle(code_dates)

        trX = list()
        trY = list()
        trX, trY = self.read_series_datas(code_dates[:self.batch_size])
        teX, teY = self.read_series_datas(code_dates[-self.test_size:])
        return trX, trY, teX, teY

    def run(self):
        trX, trY, teX, teY = self.read_datas()
        X = tf.placeholder(tf.float32, [None, self.time_step_size, self.input_vec_size])
        Y = tf.placeholder(tf.float32, [None, self.label_size])
        # get lstm_size and output 3 labels
        W = self.init_weights([self.lstm_size, self.label_size])
        B = self.init_weights([self.label_size])
        py_x, state_size = self.model(X, W, B, self.lstm_size)
        loss = tf.nn.softmax_cross_entropy_with_logits(py_x, Y)
        cost = tf.reduce_mean(loss)
        train_op = tf.train.RMSPropOptimizer(0.001, 0.9).minimize(cost)
        predict_op = tf.argmax(py_x, 1)

        # Launch the graph in a session
        with tf.Session() as sess:
            # you need to initialize all variables
            tf.global_variables_initializer().run()
            for i in range(100):
                for start, end in zip(range(0, len(trX), self.batch_size), range(self.batch_size, len(trX) + 1, self.batch_size)):
                    sess.run(train_op, feed_dict={X: trX[start:end], Y: trY[start:end]})

                test_indices = np.arange(len(teX))  # Get A Test Batch
                # np.random.shuffle(test_indices)
                test_indices = test_indices[0:self.test_size]
                org = teY[test_indices]
                res = sess.run(predict_op, feed_dict={X: teX[test_indices], Y: teY[test_indices]})
                print(i, np.mean(np.argmax(org, axis=1) == res))

tf = TFStockStudy()
tf.run()
