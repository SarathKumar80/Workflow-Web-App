using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.IO;
using System.Threading;
using System.Configuration;
using System.Text;
using System.Data.OleDb;
using System.Xml.Linq;



namespace WindowsServiceCS
{
    public partial class Service1 : ServiceBase
    {
       
        OleDbConnection Con = null;
        OleDbConnection con1 = null;
        OleDbCommand Cmd = null;
        OleDbCommand Cmd1 = null;
        OleDbCommand Cmd2 = null;
        


        public Service1()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            this.WriteToFile("Simple Service started {0}");
            this.ScheduleService();
        }

        protected override void OnStop()
        {
            this.WriteToFile("Simple Service stopped {0}");
            this.Schedular.Dispose();
        }

        private Timer Schedular;

        public void ScheduleService()
        {
            try
            {
                Schedular = new Timer(new TimerCallback(SchedularCallback));
                string mode = ConfigurationManager.AppSettings["Mode"].ToUpper();
                this.WriteToFile("Simple Service Mode: " + mode + " {0}");

                //Set the Default Time.
                DateTime scheduledTime = DateTime.MinValue;

                if (mode == "DAILY")
                {
                    //Get the Scheduled Time from AppSettings.
                    scheduledTime = DateTime.Parse(System.Configuration.ConfigurationManager.AppSettings["ScheduledTime"]);
                    if (DateTime.Now > scheduledTime)
                    {
                        //If Scheduled Time is passed set Schedule for the next day.
                        scheduledTime = scheduledTime.AddDays(1);
                    }
                }

                if (mode.ToUpper() == "INTERVAL")
                {
                    //Get the Interval in Minutes from AppSettings.
                    int intervalMinutes = Convert.ToInt32(ConfigurationManager.AppSettings["IntervalMinutes"]);

                    //Set the Scheduled Time by adding the Interval to Current Time.
                    scheduledTime = DateTime.Now.AddMinutes(intervalMinutes);
                    if (DateTime.Now > scheduledTime)
                    {
                        //If Scheduled Time is passed set Schedule for the next Interval.
                        scheduledTime = scheduledTime.AddMinutes(intervalMinutes);
                    }
                }

                TimeSpan timeSpan = scheduledTime.Subtract(DateTime.Now);
                string schedule = string.Format("{0} day(s) {1} hour(s) {2} minute(s) {3} seconds(s)", timeSpan.Days, timeSpan.Hours, timeSpan.Minutes, timeSpan.Seconds);

                this.WriteToFile("Simple Service scheduled to run after: " + schedule + " {0}");

                //Get the difference in Minutes between the Scheduled and Current Time.
                int dueTime = Convert.ToInt32(timeSpan.TotalMilliseconds);

                //Change the Timer's Due Time.
                Schedular.Change(dueTime, Timeout.Infinite);
            }
            catch (Exception ex)
            {
                WriteToFile("Simple Service Error on: {0} " + ex.Message + ex.StackTrace);

                //Stop the Windows Service.
                using (System.ServiceProcess.ServiceController serviceController = new System.ServiceProcess.ServiceController("SimpleService"))
                {
                    serviceController.Stop();
                }
            }
        }

        private void SchedularCallback(object e)
        {
            this.WriteToFile("Simple Service Log: {0}");
            this.ScheduleService();
        }

        private void WriteToFile(string text)
        {
            Con = new OleDbConnection("Provider=MSDAORA;Data Source=192.168.0.217/orcl;Persist Security Info=True;User ID=hr;Password=hr;");
            string path = "C:\\ServiceLog.txt";
            using (StreamWriter writer = new StreamWriter(path, true))
            {

                int i = 0, j = 0, k = 0, l = 0, m = 0;
                String S, S1;
                Cmd = new OleDbCommand("Select Customer_Id,Order_xml,order_id from Customer_Orders where order_status='PC' and order_type='N'", Con);
                Con.Open();

                OleDbDataReader R = Cmd.ExecuteReader();
                while (R.Read() == true)
                {


                    S = R[1].ToString();
                    XDocument X = XDocument.Parse(S);


                    var result = from E in X.Root.Elements("Table1")   //xml field name
                                 select new
                                 {
                                     fname = E.Element("customer_fname").Value,
                                     lname = E.Element("customer_lname").Value,
                                     eid = E.Element("customer_email_id").Value,
                                     mno = E.Element("customer_mobile_no").Value,
                                     status = E.Element("customer_status").Value,
                                     type = E.Element("customer_type").Value,
                                     pay = E.Element("customer_Bill_pay").Value,

                                 };
                    foreach (var e1 in result)
                    {
                        int k1 = Int32.Parse(R[0].ToString());
                        Cmd = new OleDbCommand("insert into customer(Customer_Id,customer_fname,customer_lname,customer_email_id,customer_mobile_no,customer_status,customer_type,bill_payment_method) values (" + k1 + ",?,?,?,?,?,?,?)", Con);   //oracle table name
                        Cmd.Parameters.AddWithValue("?", e1.fname);
                        Cmd.Parameters.AddWithValue("?", e1.lname);
                        Cmd.Parameters.AddWithValue("?", e1.eid);
                        Cmd.Parameters.AddWithValue("?", e1.mno);
                        Cmd.Parameters.AddWithValue("?", e1.status);
                        Cmd.Parameters.AddWithValue("?", e1.type);
                        Cmd.Parameters.AddWithValue("?", e1.pay);
                        i = Cmd.ExecuteNonQuery();



                    }



                    // For the address table
                    var result1 = from E in X.Root.Elements("Table2")
                                  select new
                                  {
                                      atype = E.Element("add_type").Value,
                                      //  aid = E.Element("add_id").Value,
                                      line1 = E.Element("add_line1").Value,
                                      line2 = E.Element("add_line2").Value,
                                      city = E.Element("city").Value,
                                      state = E.Element("state").Value,
                                      scode = E.Element("state_code").Value,
                                      zcode = E.Element("zip_code").Value,

                                  };
                    foreach (var e1 in result1)
                    {
                        Cmd = new OleDbCommand("insert into customer_address values(address_seq.nextval,?,?,?,?,?,?)", Con);
                        // Cmd.Parameters.AddWithValue("?", e1.aid);
                        Cmd.Parameters.AddWithValue("?", e1.line1);
                        Cmd.Parameters.AddWithValue("?", e1.line2);
                        Cmd.Parameters.AddWithValue("?", e1.city);
                        Cmd.Parameters.AddWithValue("?", e1.state);
                        Cmd.Parameters.AddWithValue("?", e1.scode);
                        Cmd.Parameters.AddWithValue("?", e1.zcode);

                        j = Cmd.ExecuteNonQuery();
                        Cmd1 = new OleDbCommand("Select max(Address_Id) from customer_address", Con);
                        OleDbDataReader R1 = Cmd1.ExecuteReader();
                        R1.Read();
                        if (e1.atype == "Billing")
                        {


                            Cmd = new OleDbCommand("Update Customer set Billing_Address_Id=? where Customer_Id=" + R[0].ToString(), Con);
                        }
                        else
                        {
                            Cmd = new OleDbCommand("Update Customer set Service_Address_Id=? where Customer_Id=" + R[0].ToString(), Con);
                        }
                        Cmd.Parameters.AddWithValue("?", Int32.Parse(R1[0].ToString()));
                        Cmd.ExecuteNonQuery();
                    }




                    var result2 = from E in X.Root.Elements("Table3")
                                  select new
                                  {
                                      serviceid = E.Element("service_id").Value,
                                      productid = E.Element("product_id").Value,
                                      prquan = E.Element("product_qty").Value,
                                      prdesc = E.Element("product_description").Value,
                                      psdate = E.Element("product_start_date").Value,
                                      pedate = E.Element("product_end_date").Value,


                                  };
                    foreach (var e2 in result2)
                    {
                        Cmd = new OleDbCommand("insert into product_services(customer_id,order_id,service_id,product_id,product_quantity,product_description,product_start_date,product_end_date) values(" + Int32.Parse(R[0].ToString()) + "," + Int32.Parse(R[2].ToString()) + ",?,?,?,?,?,?)", Con);
                        Cmd.Parameters.AddWithValue("?", e2.serviceid);
                        Cmd.Parameters.AddWithValue("?", e2.productid);
                        Cmd.Parameters.AddWithValue("?", e2.prquan);
                        Cmd.Parameters.AddWithValue("?", e2.prdesc);
                        string D = DateTime.Parse(e2.psdate).ToString("dd-MMM-yyyy");

                        Cmd.Parameters.AddWithValue("?", D);
                        D = DateTime.Parse(e2.pedate).ToString("dd-MMM-yyyy");
                        Cmd.Parameters.AddWithValue("?", D);
                        k = Cmd.ExecuteNonQuery();


                    }




                    var result3 = from E in X.Root.Elements("Table3")
                                  select new
                                  {
                                      serviceid1 = E.Element("service_id").Value,
                                      sedate = E.Element("service_end_date").Value,

                                  };
                    foreach (var e3 in result3)
                    {
                        Cmd = new OleDbCommand("insert into customer_services(customer_id,service_id,service_end_date) values(" + R[0].ToString() + " ,?,?)", Con);
                        Cmd.Parameters.AddWithValue("?", e3.serviceid1);
                        string D = DateTime.Parse(e3.sedate).ToString("dd-MMM-yyyy");
                        Cmd.Parameters.AddWithValue("?", D);
                        l = Cmd.ExecuteNonQuery();


                    }


                    var result4 = from E in X.Root.Elements("Table3")
                                  select new
                                  {
                                      serviceid2 = E.Element("service_id").Value,
                                      sername = E.Element("service_name").Value,

                                  };
                    foreach (var e4 in result4)
                    {
                        Cmd = new OleDbCommand("insert into service_description values(?,?)", Con);
                        Cmd.Parameters.AddWithValue("?", e4.serviceid2);
                        Cmd.Parameters.AddWithValue("?", e4.sername);
                        m = Cmd.ExecuteNonQuery();


                    }



                    if (i == 1 && j == 1 && k == 1 && l == 0 && m == 0)
                    {
                        Cmd = new OleDbCommand("update Customer_orders set order_status='BR' where order_status='PC'", Con);
                        Cmd.ExecuteNonQuery();
                    }

                }
                Con.Close();




                //Cmd = new OleDbCommand("Select Order_xml,order_id from Customer_Orders where order_type='C' and order_status='PC'", Con);
                //Con.Open();
                //OleDbDataReader R2 = Cmd.ExecuteReader();
                //while (R2.Read())
                //{

                //    S1 = R2[1].ToString();
                //    XDocument X1 = XDocument.Parse(S1);

                //    var result5 = from E in X1.Root.Elements("Table1")   //xml field name
                //                 select new
                //                 {
                //                     custid = E.Element("customer_id").Value,
                //                     servid = E.Element("service_id").Value,
                //                     prodid = E.Element("product_id").Value,
                //                     proddesc = E.Element("product_desc").Value,
                //                     prodquan = E.Element("product_quantity").Value,
                //                     ornedate = E.Element("order_negotiation_date").Value,
                //                     ordddate = E.Element("order_due_date").Value,

                //                 };
                //    foreach (var e5 in result5)
                //    {


                //        Cmd = new OleDbCommand("insert into product_services(customer_id,service_id,product_id,product_quantity,product_description,product_start_date,product_end_date) values(?,?,?,?,?,?,?)", Con);
                //        Cmd.Parameters.AddWithValue("?", e5.custid);
                //        Cmd.Parameters.AddWithValue("?", e5.servid);
                //        Cmd.Parameters.AddWithValue("?", e5.prodid);
                //        Cmd.Parameters.AddWithValue("?", e5.proddesc);
                //        Cmd.Parameters.AddWithValue("?", e5.ornedate);
                //        Cmd.Parameters.AddWithValue("?", "31-12-2999");
                //        i = Cmd.ExecuteNonQuery();





                //        //Cmd3 = new OleDbCommand("update customer_orders set order_negotiation_date=?,order_due_date=? where customer_id=? and order_id= " + R2[1].ToString() + " ", Con);
                //        //Cmd3.Parameters.AddWithValue("?", e5.ornedate);
                //        //Cmd3.Parameters.AddWithValue("?", e5.ordddate);
                //        //Cmd3.Parameters.AddWithValue("?", e5.custid);
                //        //Cmd3.ExecuteNonQuery();



                //    }
                //    Con.Close();




                //}





                writer.WriteLine(string.Format(text, DateTime.Now.ToString("dd/MM/yyyy hh:mm:ss tt")));
            writer.Close();
            }
        }
    }
}
