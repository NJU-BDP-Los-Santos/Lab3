package ReduceSide;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ProductOrder implements WritableComparable<ProductOrder>
{
    private int pid;
    private String pname;
    private int price;
    // 如上为商品的类型
    private int id;
    private String order_date;
    private int num;

    public ProductOrder(int pid, String pname, int price)
    {
        this.pid = pid;
        this.pname = pname;
        this.price = price;

        this.id = -1;
        this.order_date = "";
        this.num = -1;
    }

    public ProductOrder(int pid, int id, String order_date, int num)
    {
        this.pid = pid;
        this.id = id;
        this.order_date = order_date;
        this.num = num;

        this.pname = "";
        this.price = -1;
    }

    public ProductOrder()
    {

    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        out.writeInt(pid);
        out.writeUTF(pname);
        out.writeInt(price);

        out.writeInt(id);
        out.writeInt(num);
        out.writeUTF(order_date);
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        pid = in.readInt();
        pname = in.readUTF();
        price = in.readInt();

        id = in.readInt();
        num = in.readInt();
        order_date = in.readUTF();
    }

    @Override
    public int compareTo(ProductOrder other)
    {
        if (this.pid < other.pid)
            return -1;
        else if (this.pid > other.pid)
            return 1;
        else
        {
            // 此时是两个pid相同的情况
            if (!this.pname.equals(""))
                return -1;
            else if (!other.pname.equals(""))   // 保证这一属性有值的在前面
                return 1;
            else
                return 0;
        }
    }

    public int getPid()
    {
        return this.pid;
    }

    public void print()
    {
        System.out.println(pid + " " + pname + " " + price + " " + id + " " + order_date + " " + num);
    }
}
