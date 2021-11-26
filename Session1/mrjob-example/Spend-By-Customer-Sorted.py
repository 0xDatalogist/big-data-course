from mrjob.job import MRJob
from mrjob.step import MRStep

class SpendByCustomerSorted(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_orders,
                   reducer=self.reducer_totals_by_customer),
            MRStep(mapper=self.mapper_make_amounts_key,
                   reducer=self.reducer_output_results)
        ]
    def mapper_get_orders(self, _, line):
        (customerID, itemID, orderAmount) = line.split(',')
        yield customerID, float(orderAmount)

    def reducer_totals_by_customer(self, customerID, orders):
        yield customerID, sum(orders)

    def mapper_make_amounts_key(self, customerID, orderTotal):
        yield '%04.02f'%float(orderTotal), customerID
        
    def reducer_output_results(self, orderTotal, customerIDs):
        for customerID in customerIDs:
            yield customerID, orderTotal

if __name__ == '__main__':
    SpendByCustomerSorted.run()
    