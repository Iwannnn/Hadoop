package cn.iwannnn.mprofit;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MProfitReducer extends Reducer<Text, MProfit, Text, MProfit> {
	public void reduce(Text ikey, Iterable<MProfit> ivalues, Context context) throws IOException, InterruptedException {
		for (MProfit mProfit : ivalues) {
			mProfit.setProfit(mProfit.getIn() - mProfit.getOut());
			context.write(new Text(mProfit.getName()), mProfit);
		}
	}
}
