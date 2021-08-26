package cn.iwannnn.profit;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProfitReducer extends Reducer<Text, Profit, Text, Profit> {
	public void reduce(Text ikey, Iterable<Profit> ivalues, Context context) throws IOException, InterruptedException {
		for (Profit mProfit : ivalues) {
			mProfit.setProfit(mProfit.getIn() - mProfit.getOut());
			context.write(new Text(mProfit.getName()), mProfit);
		}
	}
}
