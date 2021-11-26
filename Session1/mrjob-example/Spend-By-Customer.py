from mrjob.job import MRJob

class SpendByCustomer(MRJob):

    def mapper(self, _, line):
        (customerID, itemID, orderAmount) = line.split(',')
        yield customerID, float(orderAmount)

    def reducer(self, customerID, orders):
        yield customerID, sum(orders)


if __name__ == '__main__':
    SpendByCustomer.run()
    