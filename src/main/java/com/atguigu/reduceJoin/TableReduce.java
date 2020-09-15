package com.atguigu.reduceJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.codehaus.jackson.map.util.BeanUtil;

import javax.xml.soap.Text;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author 贾
 * @Date 2020/8/2911:00
 */
public class TableReduce extends Reducer<Text,TableBean, NullWritable,TableBean> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        //用来接收商品表数据
        List<TableBean> orderBenas = new ArrayList<TableBean>();
        //用来接收订单表数据
        TableBean pBean = new TableBean();
        for (TableBean tableBean : values) {

            if("0".equals(tableBean.getFlag())){
                try {
                    TableBean  oBean = new TableBean();
                    BeanUtils.copyProperties(oBean,tableBean);
                    orderBenas.add(oBean);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }else {
                // pd.txt
                try {
                    BeanUtils.copyProperties(pBean,tableBean);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }

        for (TableBean orderBena : orderBenas) {
            orderBena.setpName(pBean.getpName());
            context.write(NullWritable.get(),orderBena);
        }
    }
}
