using Npgsql;
using Renci.SshNet;
using SAP.Middleware.Connector;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace TEST_SSH_Y_POSTGRES
{
    public partial class Form1 : Form
    {
        private DataSet ds = new DataSet();
        private DataTable dt = new DataTable();
        public string tabla;
        int k;


        public Form1()
        {
            InitializeComponent();

        }

        private void Form1_Load(object sender, EventArgs e)
        {

        }

        private static SAP.Middleware.Connector.RfcDestination connectSAP()
        {
            RfcDestination SapRfcDestination = RfcDestinationManager.GetDestination("DEV");
            return SapRfcDestination;
        }


        private void Button1_Click(object sender, EventArgs e)
        {
            using (var client = new SshClient("190.104.168.19", 2222, "root", "Admin1234."))
            {
                client.Connect();

                if (!client.IsConnected)
                {
                    // Display error
                    label3.Text = "No Concectado";
                }
                else
                {
                    label3.Text = "Conectado por SSH";
                }

                var port = new ForwardedPortLocal("127.0.0.1", "127.0.0.1", 5432);
                client.AddForwardedPort(port);
                port.Start();

                string connString =
                    $"Server={port.BoundHost};Database=atria;Port={port.BoundPort};" +
                     "User Id=sap_atria;Password=XEt7xAqD1F4e;";

                using (var conn = new NpgsqlConnection(connString))
                {
                    conn.Open();

                    string sql = "SELECT  * FROM detalle_de";

                    NpgsqlDataAdapter da = new NpgsqlDataAdapter(sql, conn);

                    ds.Reset();

                    da.Fill(ds);

                    dt = ds.Tables[0];

                    dataGridView1.DataSource = dt;

                    conn.Close();
                }

                port.Stop();
                client.Disconnect();
            }
        }

        

        private void ZFEI003()
        {
            tabla = "ZFEI003";
            label2.Text = "";
            k = 0;

            RfcDestination SapRfcDestination = connectSAP();
            RfcSessionManager.BeginContext(SapRfcDestination);
            IRfcFunction funcInvHdrs = SapRfcDestination.Repository.CreateFunction("ZFX0001");
            funcInvHdrs.Invoke(SapRfcDestination);
            string cntonHdrs = funcInvHdrs.GetValue(tabla).ToString();
            IRfcTable tblInvHdr = funcInvHdrs.GetTable(tabla);
            tblInvHdr.Append();

            //DELETE TABLA
            using (SqlConnection cndel = new SqlConnection(ConfigurationManager.ConnectionStrings["cn"].ConnectionString))
            {
                String sqldel = "delete from " + tabla;

                using (SqlCommand commandel = new SqlCommand(sqldel, cndel))
                {
                    cndel.Open();
                    commandel.ExecuteNonQuery();
                    cndel.Close();
                }
            }

            //INSERT TABLA
            using (SqlConnection cn = new SqlConnection(ConfigurationManager.ConnectionStrings["cn"].ConnectionString))

                for (int i = 0; i < tblInvHdr.Count; i++)
                {

                    String sql = "Insert into ZFEI003 (BUKRS,VBELN,ITIDE,DDESTIDE,DINFOEMI,DINFOFISC,DEST,DPUNEXP,DNUMDOC,DSERIENUM,DFEEMIDE,ITIPTRA,DDESTIPTRA,ITIMP,DDESTIMP,CMONEOPE,DDESMONEOPE,DCONDTICAM,DTICAM,ICONDANT,DDESCONDANT,IINDPRES,DDESINDPRES,DFECEMNR,DMODCONT,DENTCONT,DANOCONT,DSECCONT,DFECODCONT,DEMAILADMIN,INATREC,ITIOPE,CPAISREC,DDESPAISRE,ITICONTREC,DRUCREC,DDVREC,ITIPIDREC,DDTIPIDREC,DNUMIDREC,DNOMREC,DNOMFANREC,DDIRREC,DNUMCASREC,CDEPREC,DDESDEPREC,CDISREC,DDESDISREC,CCIUREC,DDESCIUREC,DTELREC,DCELREC,DEMAILREC,DEMAILADIC,DCODCLIENTE,DCICLO,DFECINIC,DFECFINC,DVENCPAG,DCONTRATO,DSALANT,DINFADIC,ICONDOPE,DDCONDOPE,ICONDCRED,DDCONDCRED,DPLAZOCRE,DCUOTAS,DMONENT) VALUES (@BUKRS,@VBELN,@ITIDE,@DDESTIDE,@DINFOEMI,@DINFOFISC,@DEST,@DPUNEXP,@DNUMDOC,@DSERIENUM,@DFEEMIDE,@ITIPTRA,@DDESTIPTRA,@ITIMP,@DDESTIMP,@CMONEOPE,@DDESMONEOPE,@DCONDTICAM,@DTICAM,@ICONDANT,@DDESCONDANT,@IINDPRES,@DDESINDPRES,@DFECEMNR,@DMODCONT,@DENTCONT,@DANOCONT,@DSECCONT,@DFECODCONT,@DEMAILADMIN,@INATREC,@ITIOPE,@CPAISREC,@DDESPAISRE,@ITICONTREC,@DRUCREC,@DDVREC,@ITIPIDREC,@DDTIPIDREC,@DNUMIDREC,@DNOMREC,@DNOMFANREC,@DDIRREC,@DNUMCASREC,@CDEPREC,@DDESDEPREC,@CDISREC,@DDESDISREC,@CCIUREC,@DDESCIUREC,@DTELREC,@DCELREC,@DEMAILREC,@DEMAILADIC,@DCODCLIENTE,@DCICLO,@DFECINIC,@DFECFINC,@DVENCPAG,@DCONTRATO,@DSALANT,@DINFADIC,@ICONDOPE,@DDCONDOPE,@ICONDCRED,@DDCONDCRED,@DPLAZOCRE,@DCUOTAS,@DMONENT)";
                   

                    using (SqlCommand command = new SqlCommand(sql, cn))
                    {
                        command.Parameters.AddWithValue("@BUKRS", tblInvHdr[i].GetValue("BUKRS"));
                        command.Parameters.AddWithValue("@VBELN", tblInvHdr[i].GetValue("VBELN"));
                        command.Parameters.AddWithValue("@ITIDE", tblInvHdr[i].GetValue("ITIDE"));
                        command.Parameters.AddWithValue("@DDESTIDE", tblInvHdr[i].GetValue("DDESTIDE"));
                        command.Parameters.AddWithValue("@DINFOEMI", tblInvHdr[i].GetValue("DINFOEMI"));
                        command.Parameters.AddWithValue("@DINFOFISC", tblInvHdr[i].GetValue("DINFOFISC"));
                        command.Parameters.AddWithValue("@DEST", tblInvHdr[i].GetValue("DEST"));
                        command.Parameters.AddWithValue("@DPUNEXP", tblInvHdr[i].GetValue("DPUNEXP"));
                        command.Parameters.AddWithValue("@DNUMDOC", tblInvHdr[i].GetValue("DNUMDOC"));
                        command.Parameters.AddWithValue("@DSERIENUM", tblInvHdr[i].GetValue("DSERIENUM"));
                        command.Parameters.AddWithValue("@DFEEMIDE", tblInvHdr[i].GetValue("DFEEMIDE"));
                        command.Parameters.AddWithValue("@ITIPTRA", tblInvHdr[i].GetValue("ITIPTRA"));
                        command.Parameters.AddWithValue("@DDESTIPTRA", tblInvHdr[i].GetValue("DDESTIPTRA"));
                        command.Parameters.AddWithValue("@ITIMP", tblInvHdr[i].GetValue("ITIMP"));
                        command.Parameters.AddWithValue("@DDESTIMP", tblInvHdr[i].GetValue("DDESTIMP"));
                        command.Parameters.AddWithValue("@CMONEOPE", tblInvHdr[i].GetValue("CMONEOPE"));
                        command.Parameters.AddWithValue("@DDESMONEOPE", tblInvHdr[i].GetValue("DDESMONEOPE"));
                        command.Parameters.AddWithValue("@DCONDTICAM", tblInvHdr[i].GetValue("DCONDTICAM"));
                        command.Parameters.AddWithValue("@DTICAM", tblInvHdr[i].GetValue("DTICAM"));
                        command.Parameters.AddWithValue("@ICONDANT", tblInvHdr[i].GetValue("ICONDANT"));
                        command.Parameters.AddWithValue("@DDESCONDANT", tblInvHdr[i].GetValue("DDESCONDANT"));
                        command.Parameters.AddWithValue("@IINDPRES", tblInvHdr[i].GetValue("IINDPRES"));
                        command.Parameters.AddWithValue("@DDESINDPRES", tblInvHdr[i].GetValue("DDESINDPRES"));
                        command.Parameters.AddWithValue("@DFECEMNR", tblInvHdr[i].GetValue("DFECEMNR"));
                        command.Parameters.AddWithValue("@DMODCONT", tblInvHdr[i].GetValue("DMODCONT"));
                        command.Parameters.AddWithValue("@DENTCONT", tblInvHdr[i].GetValue("DENTCONT"));
                        command.Parameters.AddWithValue("@DANOCONT", tblInvHdr[i].GetValue("DANOCONT"));
                        command.Parameters.AddWithValue("@DSECCONT", tblInvHdr[i].GetValue("DSECCONT"));
                        command.Parameters.AddWithValue("@DFECODCONT", tblInvHdr[i].GetValue("DFECODCONT"));
                        command.Parameters.AddWithValue("@DEMAILADMIN", tblInvHdr[i].GetValue("DEMAILADMIN"));
                        command.Parameters.AddWithValue("@INATREC", tblInvHdr[i].GetValue("INATREC"));
                        command.Parameters.AddWithValue("@ITIOPE", tblInvHdr[i].GetValue("ITIOPE"));
                        command.Parameters.AddWithValue("@CPAISREC", tblInvHdr[i].GetValue("CPAISREC"));
                        command.Parameters.AddWithValue("@DDESPAISRE", tblInvHdr[i].GetValue("DDESPAISRE"));
                        command.Parameters.AddWithValue("@ITICONTREC", tblInvHdr[i].GetValue("ITICONTREC"));
                        command.Parameters.AddWithValue("@DRUCREC", tblInvHdr[i].GetValue("DRUCREC"));
                        command.Parameters.AddWithValue("@DDVREC", tblInvHdr[i].GetValue("DDVREC"));
                        command.Parameters.AddWithValue("@ITIPIDREC", tblInvHdr[i].GetValue("ITIPIDREC"));
                        command.Parameters.AddWithValue("@DDTIPIDREC", tblInvHdr[i].GetValue("DDTIPIDREC"));
                        command.Parameters.AddWithValue("@DNUMIDREC", tblInvHdr[i].GetValue("DNUMIDREC"));
                        command.Parameters.AddWithValue("@DNOMREC", tblInvHdr[i].GetValue("DNOMREC"));
                        command.Parameters.AddWithValue("@DNOMFANREC", tblInvHdr[i].GetValue("DNOMFANREC"));
                        command.Parameters.AddWithValue("@DDIRREC", tblInvHdr[i].GetValue("DDIRREC"));
                        command.Parameters.AddWithValue("@DNUMCASREC", tblInvHdr[i].GetValue("DNUMCASREC"));
                        command.Parameters.AddWithValue("@CDEPREC", tblInvHdr[i].GetValue("CDEPREC"));
                        command.Parameters.AddWithValue("@DDESDEPREC", tblInvHdr[i].GetValue("DDESDEPREC"));
                        command.Parameters.AddWithValue("@CDISREC", tblInvHdr[i].GetValue("CDISREC"));
                        command.Parameters.AddWithValue("@DDESDISREC", tblInvHdr[i].GetValue("DDESDISREC"));
                        command.Parameters.AddWithValue("@CCIUREC", tblInvHdr[i].GetValue("CCIUREC"));
                        command.Parameters.AddWithValue("@DDESCIUREC", tblInvHdr[i].GetValue("DDESCIUREC"));
                        command.Parameters.AddWithValue("@DTELREC", tblInvHdr[i].GetValue("DTELREC"));
                        command.Parameters.AddWithValue("@DCELREC", tblInvHdr[i].GetValue("DCELREC"));
                        command.Parameters.AddWithValue("@DEMAILREC", tblInvHdr[i].GetValue("DEMAILREC"));
                        command.Parameters.AddWithValue("@DEMAILADIC", tblInvHdr[i].GetValue("DEMAILADIC"));
                        command.Parameters.AddWithValue("@DCODCLIENTE", tblInvHdr[i].GetValue("DCODCLIENTE"));
                        command.Parameters.AddWithValue("@DCICLO", tblInvHdr[i].GetValue("DCICLO"));
                        command.Parameters.AddWithValue("@DFECINIC", tblInvHdr[i].GetValue("DFECINIC"));
                        command.Parameters.AddWithValue("@DFECFINC", tblInvHdr[i].GetValue("DFECFINC"));
                        command.Parameters.AddWithValue("@DVENCPAG", tblInvHdr[i].GetValue("DVENCPAG"));
                        command.Parameters.AddWithValue("@DCONTRATO", tblInvHdr[i].GetValue("DCONTRATO"));
                        command.Parameters.AddWithValue("@DSALANT", tblInvHdr[i].GetValue("DSALANT"));
                        command.Parameters.AddWithValue("@DINFADIC", tblInvHdr[i].GetValue("DINFADIC"));
                        command.Parameters.AddWithValue("@ICONDOPE", tblInvHdr[i].GetValue("ICONDOPE"));
                        command.Parameters.AddWithValue("@DDCONDOPE", tblInvHdr[i].GetValue("DDCONDOPE"));
                        command.Parameters.AddWithValue("@ICONDCRED", tblInvHdr[i].GetValue("ICONDCRED"));
                        command.Parameters.AddWithValue("@DDCONDCRED", tblInvHdr[i].GetValue("DDCONDCRED"));
                        command.Parameters.AddWithValue("@DPLAZOCRE", tblInvHdr[i].GetValue("DPLAZOCRE"));
                        command.Parameters.AddWithValue("@DCUOTAS", tblInvHdr[i].GetValue("DCUOTAS"));
                        command.Parameters.AddWithValue("@DMONENT", tblInvHdr[i].GetValue("DMONENT"));


                        cn.Open();
                        command.ExecuteNonQuery();
                        cn.Close();

                    }

                    k = i;
                    label2.Text = k + " Registros insertados en " + tabla;

                }



            ///////////////// CONEXION A POSTGRE //////////////////////////////////////////////////////////////////////////////////////////////////////////////
            using (SqlConnection cndel = new SqlConnection(ConfigurationManager.ConnectionStrings["cn"].ConnectionString))
            {


                String sqlsel = "SELECT ITIDE,DDESTIDE,DINFOEMI,DINFOFISC,DEST,DPUNEXP,DNUMDOC,DSERIENUM,DFEEMIDE,ITIPTRA,DDESTIPTRA,ITIMP,DDESTIMP,CMONEOPE,DDESMONEOPE,DCONDTICAM,DTICAM,ICONDANT,DDESCONDANT,IINDPRES,DDESINDPRES,DFECEMNR,DMODCONT,DENTCONT,DANOCONT,DSECCONT,DFECODCONT,DEMAILADMIN,INATREC,ITIOPE,CPAISREC,DDESPAISRE,ITICONTREC,DRUCREC,DDVREC,ITIPIDREC,DDTIPIDREC,DNUMIDREC,DNOMREC,DNOMFANREC,DDIRREC,DNUMCASREC,CDEPREC,DDESDEPREC,CDISREC,DDESDISREC,CCIUREC,DDESCIUREC,DTELREC,DCELREC,DEMAILREC,DEMAILADIC,DCODCLIENTE,DCICLO,DFECINIC,DFECFINC,DVENCPAG,DCONTRATO,DSALANT,DINFADIC,ICONDOPE,DDCONDOPE,ICONDCRED,DDCONDCRED,DPLAZOCRE,DCUOTAS,DMONENT from " + tabla + " WHERE BUKRS='UBPY'";


                SqlDataAdapter dataAdapter = new SqlDataAdapter(sqlsel, cndel);

                cndel.Open();
                DataTable dt1 = new DataTable();

                dataAdapter.Fill(dt1);

                //dataGridView2.DataSource = dt1; // MUESTRA EL RESULTADO, SI HAY ALGO 

                cndel.Close();



                using (var client = new SshClient("190.104.168.19", 2222, "root", "Admin1234."))
                {
                    client.Connect();

                    if (!client.IsConnected)
                    {
                        // Display error
                        label3.Text = "No Concectado";
                    }
                    else
                    {
                        label3.Text = "Conectado por SSH";
                    }

                    var port = new ForwardedPortLocal("127.0.0.1", "127.0.0.1", 5432);
                    client.AddForwardedPort(port);
                    port.Start();

                    string connString =
                        $"Server={port.BoundHost};Database=atria;Port={port.BoundPort};" +
                         "User Id=sap_atria;Password=XEt7xAqD1F4e;";

                    using (var conn = new NpgsqlConnection(connString))
                    {
                        conn.Open();


                        foreach (DataRow row in dt1.Rows)
                        {
                            string sql = "INSERT INTO t_factura_cab (ITIDE,DDESTIDE,DINFOEMI,DINFOFISC,DEST,DPUNEXP,DNUMDOC,DSERIENUM,DFEEMIDE,ITIPTRA,DDESTIPTRA,ITIMP,DDESTIMP,CMONEOPE,DDESMONEOPE,DCONDTICAM,DTICAM,ICONDANT,DDESCONDANT,IINDPRES,DDESINDPRES,DFECEMNR,DMODCONT,DENTCONT,DANOCONT,DSECCONT,DFECODCONT,DEMAILADMIN,INATREC,ITIOPE,CPAISREC,DDESPAISRE,ITICONTREC,DRUCREC,DDVREC,ITIPIDREC,DDTIPIDREC,DNUMIDREC,DNOMREC,DNOMFANREC,DDIRREC,DNUMCASREC,CDEPREC,DDESDEPREC,CDISREC,DDESDISREC,CCIUREC,DDESCIUREC,DTELREC,DCELREC,DEMAILREC,DEMAILADIC,DCODCLIENTE,DCICLO,DFECINIC,DFECFINC,DVENCPAG,DCONTRATO,DSALANT,DINFADIC,ICONDOPE,DDCONDOPE,ICONDCRED,DDCONDCRED,DPLAZOCRE,DCUOTAS,DMONENT) VALUES (@ITIDE,@DDESTIDE,@DINFOEMI,@DINFOFISC,@DEST,@DPUNEXP,@DNUMDOC,@DSERIENUM,@DFEEMIDE,@ITIPTRA,@DDESTIPTRA,@ITIMP,@DDESTIMP,@CMONEOPE,@DDESMONEOPE,@DCONDTICAM,@DTICAM,@ICONDANT,@DDESCONDANT,@IINDPRES,@DDESINDPRES,@DFECEMNR,@DMODCONT,@DENTCONT,@DANOCONT,@DSECCONT,@DFECODCONT,@DEMAILADMIN,@INATREC,@ITIOPE,@CPAISREC,@DDESPAISRE,@ITICONTREC,@DRUCREC,@DDVREC,@ITIPIDREC,@DDTIPIDREC,@DNUMIDREC,@DNOMREC,@DNOMFANREC,@DDIRREC,@DNUMCASREC,@CDEPREC,@DDESDEPREC,@CDISREC,@DDESDISREC,@CCIUREC,@DDESCIUREC,@DTELREC,@DCELREC,@DEMAILREC,@DEMAILADIC,@DCODCLIENTE,@DCICLO,@DFECINIC,@DFECFINC,@DVENCPAG,@DCONTRATO,@DSALANT,@DINFADIC,@ICONDOPE,@DDCONDOPE,@ICONDCRED,@DDCONDCRED,@DPLAZOCRE,@DCUOTAS,@DMONENT)";

                            NpgsqlDataAdapter da = new NpgsqlDataAdapter(sql, conn);
                            using (NpgsqlCommand command = new NpgsqlCommand(sql, conn))
                            {
                                command.Parameters.AddWithValue("@ITIDE", row.Field<string>("ITIDE"));
                                command.Parameters.AddWithValue("@DDESTIDE", row.Field<string>("DDESTIDE"));
                                command.Parameters.AddWithValue("@DINFOEMI", row.Field<string>("DINFOEMI"));
                                command.Parameters.AddWithValue("@DINFOFISC", row.Field<string>("DINFOFISC"));
                                command.Parameters.AddWithValue("@DEST", row.Field<string>("DEST"));
                                command.Parameters.AddWithValue("@DPUNEXP", row.Field<string>("DPUNEXP"));
                                command.Parameters.AddWithValue("@DNUMDOC", row.Field<string>("DNUMDOC"));
                                command.Parameters.AddWithValue("@DSERIENUM", row.Field<string>("DSERIENUM"));
                                command.Parameters.AddWithValue("@DFEEMIDE", row.Field<string>("DFEEMIDE"));
                                command.Parameters.AddWithValue("@ITIPTRA", row.Field<string>("ITIPTRA"));
                                command.Parameters.AddWithValue("@DDESTIPTRA", row.Field<string>("DDESTIPTRA"));
                                command.Parameters.AddWithValue("@ITIMP", row.Field<string>("ITIMP"));
                                command.Parameters.AddWithValue("@DDESTIMP", row.Field<string>("DDESTIMP"));
                                command.Parameters.AddWithValue("@CMONEOPE", row.Field<string>("CMONEOPE"));
                                command.Parameters.AddWithValue("@DDESMONEOPE", row.Field<string>("DDESMONEOPE"));
                                command.Parameters.AddWithValue("@DCONDTICAM", row.Field<string>("DCONDTICAM"));
                                command.Parameters.AddWithValue("@DTICAM", row.Field<string>("DTICAM"));
                                command.Parameters.AddWithValue("@ICONDANT", row.Field<string>("ICONDANT"));
                                command.Parameters.AddWithValue("@DDESCONDANT", row.Field<string>("DDESCONDANT"));
                                command.Parameters.AddWithValue("@IINDPRES", row.Field<string>("IINDPRES"));
                                command.Parameters.AddWithValue("@DDESINDPRES", row.Field<string>("DDESINDPRES"));
                                command.Parameters.AddWithValue("@DFECEMNR", row.Field<string>("DFECEMNR"));
                                command.Parameters.AddWithValue("@DMODCONT", row.Field<string>("DMODCONT"));
                                command.Parameters.AddWithValue("@DENTCONT", row.Field<string>("DENTCONT"));
                                command.Parameters.AddWithValue("@DANOCONT", row.Field<string>("DANOCONT"));
                                command.Parameters.AddWithValue("@DSECCONT", row.Field<string>("DSECCONT"));
                                command.Parameters.AddWithValue("@DFECODCONT", row.Field<string>("DFECODCONT"));
                                command.Parameters.AddWithValue("@DEMAILADMIN", row.Field<string>("DEMAILADMIN"));
                                command.Parameters.AddWithValue("@INATREC", row.Field<string>("INATREC"));
                                command.Parameters.AddWithValue("@ITIOPE", row.Field<string>("ITIOPE"));
                                command.Parameters.AddWithValue("@CPAISREC", row.Field<string>("CPAISREC"));
                                command.Parameters.AddWithValue("@DDESPAISRE", row.Field<string>("DDESPAISRE"));
                                command.Parameters.AddWithValue("@ITICONTREC", row.Field<string>("ITICONTREC"));
                                command.Parameters.AddWithValue("@DRUCREC", row.Field<string>("DRUCREC"));
                                command.Parameters.AddWithValue("@DDVREC", row.Field<string>("DDVREC"));
                                command.Parameters.AddWithValue("@ITIPIDREC", row.Field<string>("ITIPIDREC"));
                                command.Parameters.AddWithValue("@DDTIPIDREC", row.Field<string>("DDTIPIDREC"));
                                command.Parameters.AddWithValue("@DNUMIDREC", row.Field<string>("DNUMIDREC"));
                                command.Parameters.AddWithValue("@DNOMREC", row.Field<string>("DNOMREC"));
                                command.Parameters.AddWithValue("@DNOMFANREC", row.Field<string>("DNOMFANREC"));
                                command.Parameters.AddWithValue("@DDIRREC", row.Field<string>("DDIRREC"));
                                command.Parameters.AddWithValue("@DNUMCASREC", row.Field<string>("DNUMCASREC"));
                                command.Parameters.AddWithValue("@CDEPREC", row.Field<string>("CDEPREC"));
                                command.Parameters.AddWithValue("@DDESDEPREC", row.Field<string>("DDESDEPREC"));
                                command.Parameters.AddWithValue("@CDISREC", row.Field<string>("CDISREC"));
                                command.Parameters.AddWithValue("@DDESDISREC", row.Field<string>("DDESDISREC"));
                                command.Parameters.AddWithValue("@CCIUREC", row.Field<string>("CCIUREC"));
                                command.Parameters.AddWithValue("@DDESCIUREC", row.Field<string>("DDESCIUREC"));
                                command.Parameters.AddWithValue("@DTELREC", row.Field<string>("DTELREC"));
                                command.Parameters.AddWithValue("@DCELREC", row.Field<string>("DCELREC"));
                                command.Parameters.AddWithValue("@DEMAILREC", row.Field<string>("DEMAILREC"));
                                command.Parameters.AddWithValue("@DEMAILADIC", row.Field<string>("DEMAILADIC"));
                                command.Parameters.AddWithValue("@DCODCLIENTE", row.Field<string>("DCODCLIENTE"));
                                command.Parameters.AddWithValue("@DCICLO", row.Field<string>("DCICLO"));
                                command.Parameters.AddWithValue("@DFECINIC", row.Field<string>("DFECINIC"));
                                command.Parameters.AddWithValue("@DFECFINC", row.Field<string>("DFECFINC"));
                                command.Parameters.AddWithValue("@DVENCPAG", row.Field<string>("DVENCPAG"));
                                command.Parameters.AddWithValue("@DCONTRATO", row.Field<string>("DCONTRATO"));
                                command.Parameters.AddWithValue("@DSALANT", row.Field<string>("DSALANT"));
                                command.Parameters.AddWithValue("@DINFADIC", row.Field<string>("DINFADIC"));
                                command.Parameters.AddWithValue("@ICONDOPE", row.Field<string>("ICONDOPE"));
                                command.Parameters.AddWithValue("@DDCONDOPE", row.Field<string>("DDCONDOPE"));
                                command.Parameters.AddWithValue("@ICONDCRED", row.Field<string>("ICONDCRED"));
                                command.Parameters.AddWithValue("@DDCONDCRED", row.Field<string>("DDCONDCRED"));
                                command.Parameters.AddWithValue("@DPLAZOCRE", row.Field<string>("DPLAZOCRE"));
                                command.Parameters.AddWithValue("@DCUOTAS", row.Field<string>("DCUOTAS"));
                                command.Parameters.AddWithValue("@DMONENT", row.Field<string>("DMONENT"));
                                                                
                            }
                        }

                        conn.Close();
                    }

                    label3.Text = "insertado!";
                    port.Stop();
                    client.Disconnect();
                }



            }

            //////////////////////////////////////////////////////////////////////////////////////////////////
            //////////////////////////////////////////////////////////////////////////////////////////////////



            RfcSessionManager.EndContext(SapRfcDestination);
            MOSTRAR_TABLA(tabla);
            INSERTLOG(tabla);

            


        }

            

        private void ZFEI004()
        {
            tabla = "ZFEI004";
            label2.Text = "";
            k = 0;

            RfcDestination SapRfcDestination = connectSAP();
            RfcSessionManager.BeginContext(SapRfcDestination);
            IRfcFunction funcInvHdrs = SapRfcDestination.Repository.CreateFunction("ZFX0001");
            funcInvHdrs.Invoke(SapRfcDestination);
            string cntonHdrs = funcInvHdrs.GetValue(tabla).ToString();
            IRfcTable tblInvHdr = funcInvHdrs.GetTable(tabla);
            tblInvHdr.Append();

            //DELETE TABLA
            using (SqlConnection cndel = new SqlConnection(ConfigurationManager.ConnectionStrings["cn"].ConnectionString))
            {
                String sqldel = "delete from " + tabla;

                using (SqlCommand commandel = new SqlCommand(sqldel, cndel))
                {
                    cndel.Open();
                    commandel.ExecuteNonQuery();
                    cndel.Close();
                }
            }

            //INSERT TABLA
            using (SqlConnection cn = new SqlConnection(ConfigurationManager.ConnectionStrings["cn"].ConnectionString))

                for (int i = 0; i < tblInvHdr.Count; i++)
                {

                    String sql = "Insert into " + tabla + " (BUKRS,DCODINT,DPARARANC,DNCM,DDNCPG,DDNCPE,DGTIN,DGTINPQ,DDESPROSER,CUNIMED,DDESUNIMED,DCANTPROSER,CPAISORIG,DDESPAISORIG,DINFITEM,DCDCANTICIPO,DPUNIPROSER,DTICAMIT,DTOTBRUOPEITEM,DDESCITEM,DPORCDESIT,DDESCGLOITEM,DANTPREUNIIT,DANTGLOPREUNIIT,DTOTOPEITEM,DTOTOPEGS,IAFECIVA,DDESAFECIVA,DPROPIVA,DTASAIVA,DBASGRAVIVA,DLIQIVAITEM,DNUMLOTE,DVENCMERC,DNSERIE,DNUMPEDI,DNUMSEGUI,DNOMIMP,DDIRIMP,DNUMFIR,DNUMREG,DNUMREGENTCOM,FK_FAC) VALUES (@BUKRS,@DCODINT,@DPARARANC,@DNCM,@DDNCPG,@DDNCPE,@DGTIN,@DGTINPQ,@DDESPROSER,@CUNIMED,@DDESUNIMED,@DCANTPROSER,@CPAISORIG,@DDESPAISORIG,@DINFITEM,@DCDCANTICIPO,@DPUNIPROSER,@DTICAMIT,@DTOTBRUOPEITEM,@DDESCITEM,@DPORCDESIT,@DDESCGLOITEM,@DANTPREUNIIT,@DANTGLOPREUNIIT,@DTOTOPEITEM,@DTOTOPEGS,@IAFECIVA,@DDESAFECIVA,@DPROPIVA,@DTASAIVA,@DBASGRAVIVA,@DLIQIVAITEM,@DNUMLOTE,@DVENCMERC,@DNSERIE,@DNUMPEDI,@DNUMSEGUI,@DNOMIMP,@DDIRIMP,@DNUMFIR,@DNUMREG,@DNUMREGENTCOM,@FK_FAC)";

                    using (SqlCommand command = new SqlCommand(sql, cn))
                    {
                        command.Parameters.AddWithValue("@BUKRS", tblInvHdr[i].GetValue("BUKRS"));
                        command.Parameters.AddWithValue("@DCODINT", tblInvHdr[i].GetValue("DCODINT"));
                        command.Parameters.AddWithValue("@DPARARANC", tblInvHdr[i].GetValue("DPARARANC"));
                        command.Parameters.AddWithValue("@DNCM", tblInvHdr[i].GetValue("DNCM"));
                        command.Parameters.AddWithValue("@DDNCPG", tblInvHdr[i].GetValue("DDNCPG"));
                        command.Parameters.AddWithValue("@DDNCPE", tblInvHdr[i].GetValue("DDNCPE"));
                        command.Parameters.AddWithValue("@DGTIN", tblInvHdr[i].GetValue("DGTIN"));
                        command.Parameters.AddWithValue("@DGTINPQ", tblInvHdr[i].GetValue("DGTINPQ"));
                        command.Parameters.AddWithValue("@DDESPROSER", tblInvHdr[i].GetValue("DDESPROSER"));
                        command.Parameters.AddWithValue("@CUNIMED", tblInvHdr[i].GetValue("CUNIMED"));
                        command.Parameters.AddWithValue("@DDESUNIMED", tblInvHdr[i].GetValue("DDESUNIMED"));
                        command.Parameters.AddWithValue("@DCANTPROSER", tblInvHdr[i].GetValue("DCANTPROSER"));
                        command.Parameters.AddWithValue("@CPAISORIG", tblInvHdr[i].GetValue("CPAISORIG"));
                        command.Parameters.AddWithValue("@DDESPAISORIG", tblInvHdr[i].GetValue("DDESPAISORIG"));
                        command.Parameters.AddWithValue("@DINFITEM", tblInvHdr[i].GetValue("DINFITEM"));
                        command.Parameters.AddWithValue("@DCDCANTICIPO", tblInvHdr[i].GetValue("DCDCANTICIPO"));
                        command.Parameters.AddWithValue("@DPUNIPROSER", tblInvHdr[i].GetValue("DPUNIPROSER"));
                        command.Parameters.AddWithValue("@DTICAMIT", tblInvHdr[i].GetValue("DTICAMIT"));
                        command.Parameters.AddWithValue("@DTOTBRUOPEITEM", tblInvHdr[i].GetValue("DTOTBRUOPEITEM"));
                        command.Parameters.AddWithValue("@DDESCITEM", tblInvHdr[i].GetValue("DDESCITEM"));
                        command.Parameters.AddWithValue("@DPORCDESIT", tblInvHdr[i].GetValue("DPORCDESIT"));
                        command.Parameters.AddWithValue("@DDESCGLOITEM", tblInvHdr[i].GetValue("DDESCGLOITEM"));
                        command.Parameters.AddWithValue("@DANTPREUNIIT", tblInvHdr[i].GetValue("DANTPREUNIIT"));
                        command.Parameters.AddWithValue("@DANTGLOPREUNIIT", tblInvHdr[i].GetValue("DANTGLOPREUNIIT"));
                        command.Parameters.AddWithValue("@DTOTOPEITEM", tblInvHdr[i].GetValue("DTOTOPEITEM"));
                        command.Parameters.AddWithValue("@DTOTOPEGS", tblInvHdr[i].GetValue("DTOTOPEGS"));
                        command.Parameters.AddWithValue("@IAFECIVA", tblInvHdr[i].GetValue("IAFECIVA"));
                        command.Parameters.AddWithValue("@DDESAFECIVA", tblInvHdr[i].GetValue("DDESAFECIVA"));
                        command.Parameters.AddWithValue("@DPROPIVA", tblInvHdr[i].GetValue("DPROPIVA"));
                        command.Parameters.AddWithValue("@DTASAIVA", tblInvHdr[i].GetValue("DTASAIVA"));
                        command.Parameters.AddWithValue("@DBASGRAVIVA", tblInvHdr[i].GetValue("DBASGRAVIVA"));
                        command.Parameters.AddWithValue("@DLIQIVAITEM", tblInvHdr[i].GetValue("DLIQIVAITEM"));
                        command.Parameters.AddWithValue("@DNUMLOTE", tblInvHdr[i].GetValue("DNUMLOTE"));
                        command.Parameters.AddWithValue("@DVENCMERC", tblInvHdr[i].GetValue("DVENCMERC"));
                        command.Parameters.AddWithValue("@DNSERIE", tblInvHdr[i].GetValue("DNSERIE"));
                        command.Parameters.AddWithValue("@DNUMPEDI", tblInvHdr[i].GetValue("DNUMPEDI"));
                        command.Parameters.AddWithValue("@DNUMSEGUI", tblInvHdr[i].GetValue("DNUMSEGUI"));
                        command.Parameters.AddWithValue("@DNOMIMP", tblInvHdr[i].GetValue("DNOMIMP"));
                        command.Parameters.AddWithValue("@DDIRIMP", tblInvHdr[i].GetValue("DDIRIMP"));
                        command.Parameters.AddWithValue("@DNUMFIR", tblInvHdr[i].GetValue("DNUMFIR"));
                        command.Parameters.AddWithValue("@DNUMREG", tblInvHdr[i].GetValue("DNUMREG"));
                        command.Parameters.AddWithValue("@DNUMREGENTCOM", tblInvHdr[i].GetValue("DNUMREGENTCOM"));
                        command.Parameters.AddWithValue("@FK_FAC", tblInvHdr[i].GetValue("FK_FAC"));




                        cn.Open();
                        command.ExecuteNonQuery();
                        cn.Close();

                    }

                    k = i;
                    label2.Text = k + " Registros insertados en " + tabla;

                }

            

            ///////////////// CONEXION A POSTGRE //////////////////////////////////////////////////////////////////////////////////////////////////////////////
            using (SqlConnection cndel = new SqlConnection(ConfigurationManager.ConnectionStrings["cn"].ConnectionString))
            {


                String sqlsel = "SELECT DCODINT,DPARARANC,DNCM,DDNCPG,DDNCPE,DGTIN,DGTINPQ,DDESPROSER,CUNIMED,DDESUNIMED,DCANTPROSER,CPAISORIG,DDESPAISORIG,DINFITEM,DCDCANTICIPO,DPUNIPROSER,DTICAMIT,DTOTBRUOPEITEM,DDESCITEM,DPORCDESIT,DDESCGLOITEM,DANTPREUNIIT,DANTGLOPREUNIIT,DTOTOPEITEM,DTOTOPEGS,IAFECIVA,DDESAFECIVA,DPROPIVA,DTASAIVA,DBASGRAVIVA,DLIQIVAITEM,DNUMLOTE,DVENCMERC,DNSERIE,DNUMPEDI,DNUMSEGUI,DNOMIMP,DDIRIMP,DNUMFIR,DNUMREG,DNUMREGENTCOM,FK_FAC from " + tabla + " WHERE BUKRS='UBPY'";


                SqlDataAdapter dataAdapter = new SqlDataAdapter(sqlsel, cndel);

                cndel.Open();
                DataTable dt1 = new DataTable();

                dataAdapter.Fill(dt1);

                //dataGridView2.DataSource = dt1; // MUESTRA EL RESULTADO, SI HAY ALGO 

                cndel.Close();



                using (var client = new SshClient("190.104.168.19", 2222, "root", "Admin1234."))
                {
                    client.Connect();

                    if (!client.IsConnected)
                    {
                        // Display error
                        label3.Text = "No Concectado";
                    }
                    else
                    {
                        label3.Text = "Conectado por SSH";
                    }

                    var port = new ForwardedPortLocal("127.0.0.1", "127.0.0.1", 5432);
                    client.AddForwardedPort(port);
                    port.Start();

                    string connString =
                        $"Server={port.BoundHost};Database=atria;Port={port.BoundPort};" +
                         "User Id=sap_atria;Password=XEt7xAqD1F4e;";

                    using (var conn = new NpgsqlConnection(connString))
                    {
                        conn.Open();


                        foreach (DataRow row in dt1.Rows)
                        {
                            string sql = "INSERT INTO t_factura_det (DCODINT,DPARARANC,DNCM,DDNCPG,DDNCPE,DGTIN,DGTINPQ,DDESPROSER,CUNIMED,DDESUNIMED,DCANTPROSER,CPAISORIG,DDESPAISORIG,DINFITEM,DCDCANTICIPO,DPUNIPROSER,DTICAMIT,DTOTBRUOPEITEM,DDESCITEM,DPORCDESIT,DDESCGLOITEM,DANTPREUNIIT,DANTGLOPREUNIIT,DTOTOPEITEM,DTOTOPEGS,IAFECIVA,DDESAFECIVA,DPROPIVA,DTASAIVA,DBASGRAVIVA,DLIQIVAITEM,DNUMLOTE,DVENCMERC,DNSERIE,DNUMPEDI,DNUMSEGUI,DNOMIMP,DDIRIMP,DNUMFIR,DNUMREG,DNUMREGENTCOM,FK_FAC) VALUES (@DCODINT,@DPARARANC,@DNCM,@DDNCPG,@DDNCPE,@DGTIN,@DGTINPQ,@DDESPROSER,@CUNIMED,@DDESUNIMED,@DCANTPROSER,@CPAISORIG,@DDESPAISORIG,@DINFITEM,@DCDCANTICIPO,@DPUNIPROSER,@DTICAMIT,@DTOTBRUOPEITEM,@DDESCITEM,@DPORCDESIT,@DDESCGLOITEM,@DANTPREUNIIT,@DANTGLOPREUNIIT,@DTOTOPEITEM,@DTOTOPEGS,@IAFECIVA,@DDESAFECIVA,@DPROPIVA,@DTASAIVA,@DBASGRAVIVA,@DLIQIVAITEM,@DNUMLOTE,@DVENCMERC,@DNSERIE,@DNUMPEDI,@DNUMSEGUI,@DNOMIMP,@DDIRIMP,@DNUMFIR,@DNUMREG,@DNUMREGENTCOM,@FK_FAC)";

                            NpgsqlDataAdapter da = new NpgsqlDataAdapter(sql, conn);
                            using (NpgsqlCommand command = new NpgsqlCommand(sql, conn))
                            {
                                command.Parameters.AddWithValue("@DCODINT", row.Field<string>("DCODINT"));
                                command.Parameters.AddWithValue("@DPARARANC", row.Field<string>("DPARARANC"));
                                command.Parameters.AddWithValue("@DNCM", row.Field<string>("DNCM"));
                                command.Parameters.AddWithValue("@DDNCPG", row.Field<string>("DDNCPG"));
                                command.Parameters.AddWithValue("@DDNCPE", row.Field<string>("DDNCPE"));
                                command.Parameters.AddWithValue("@DGTIN", row.Field<string>("DGTIN"));
                                command.Parameters.AddWithValue("@DGTINPQ", row.Field<string>("DGTINPQ"));
                                command.Parameters.AddWithValue("@DDESPROSER", row.Field<string>("DDESPROSER"));
                                command.Parameters.AddWithValue("@CUNIMED", row.Field<string>("CUNIMED"));
                                command.Parameters.AddWithValue("@DDESUNIMED", row.Field<string>("DDESUNIMED"));
                                command.Parameters.AddWithValue("@DCANTPROSER", row.Field<string>("DCANTPROSER"));
                                command.Parameters.AddWithValue("@CPAISORIG", row.Field<string>("CPAISORIG"));
                                command.Parameters.AddWithValue("@DDESPAISORIG", row.Field<string>("DDESPAISORIG"));
                                command.Parameters.AddWithValue("@DINFITEM", row.Field<string>("DINFITEM"));
                                command.Parameters.AddWithValue("@DCDCANTICIPO", row.Field<string>("DCDCANTICIPO"));
                                command.Parameters.AddWithValue("@DPUNIPROSER", row.Field<string>("DPUNIPROSER"));
                                command.Parameters.AddWithValue("@DTICAMIT", row.Field<string>("DTICAMIT"));
                                command.Parameters.AddWithValue("@DTOTBRUOPEITEM", row.Field<string>("DTOTBRUOPEITEM"));
                                command.Parameters.AddWithValue("@DDESCITEM", row.Field<string>("DDESCITEM"));
                                command.Parameters.AddWithValue("@DPORCDESIT", row.Field<string>("DPORCDESIT"));
                                command.Parameters.AddWithValue("@DDESCGLOITEM", row.Field<string>("DDESCGLOITEM"));
                                command.Parameters.AddWithValue("@DANTPREUNIIT", row.Field<string>("DANTPREUNIIT"));
                                command.Parameters.AddWithValue("@DANTGLOPREUNIIT", row.Field<string>("DANTGLOPREUNIIT"));
                                command.Parameters.AddWithValue("@DTOTOPEITEM", row.Field<string>("DTOTOPEITEM"));
                                command.Parameters.AddWithValue("@DTOTOPEGS", row.Field<string>("DTOTOPEGS"));
                                command.Parameters.AddWithValue("@IAFECIVA", row.Field<string>("IAFECIVA"));
                                command.Parameters.AddWithValue("@DDESAFECIVA", row.Field<string>("DDESAFECIVA"));
                                command.Parameters.AddWithValue("@DPROPIVA", row.Field<string>("DPROPIVA"));
                                command.Parameters.AddWithValue("@DTASAIVA", row.Field<string>("DTASAIVA"));
                                command.Parameters.AddWithValue("@DBASGRAVIVA", row.Field<string>("DBASGRAVIVA"));
                                command.Parameters.AddWithValue("@DLIQIVAITEM", row.Field<string>("DLIQIVAITEM"));
                                command.Parameters.AddWithValue("@DNUMLOTE", row.Field<string>("DNUMLOTE"));
                                command.Parameters.AddWithValue("@DVENCMERC", row.Field<string>("DVENCMERC"));
                                command.Parameters.AddWithValue("@DNSERIE", row.Field<string>("DNSERIE"));
                                command.Parameters.AddWithValue("@DNUMPEDI", row.Field<string>("DNUMPEDI"));
                                command.Parameters.AddWithValue("@DNUMSEGUI", row.Field<string>("DNUMSEGUI"));
                                command.Parameters.AddWithValue("@DNOMIMP", row.Field<string>("DNOMIMP"));
                                command.Parameters.AddWithValue("@DDIRIMP", row.Field<string>("DDIRIMP"));
                                command.Parameters.AddWithValue("@DNUMFIR", row.Field<string>("DNUMFIR"));
                                command.Parameters.AddWithValue("@DNUMREG", row.Field<string>("DNUMREG"));
                                command.Parameters.AddWithValue("@DNUMREGENTCOM", row.Field<string>("DNUMREGENTCOM"));
                                command.Parameters.AddWithValue("@FK_FAC", row.Field<string>("FK_FAC"));
                                

                            }
                        }

                        conn.Close();
                    }

                    label3.Text = "insertado!";
                    port.Stop();
                    client.Disconnect();
                }



            }

            //////////////////////////////////////////////////////////////////////////////////////////////////
            //////////////////////////////////////////////////////////////////////////////////////////////////



            RfcSessionManager.EndContext(SapRfcDestination);
            MOSTRAR_TABLA(tabla);
            INSERTLOG(tabla);
        }

        private void ZFEI005()
        {
            tabla = "ZFEI005";
            label2.Text = "";
            k = 0;

            RfcDestination SapRfcDestination = connectSAP();
            RfcSessionManager.BeginContext(SapRfcDestination);
            IRfcFunction funcInvHdrs = SapRfcDestination.Repository.CreateFunction("ZFX0001");
            funcInvHdrs.Invoke(SapRfcDestination);
            string cntonHdrs = funcInvHdrs.GetValue(tabla).ToString();
            IRfcTable tblInvHdr = funcInvHdrs.GetTable(tabla);
            tblInvHdr.Append();

            //DELETE TABLA
            using (SqlConnection cndel = new SqlConnection(ConfigurationManager.ConnectionStrings["cn"].ConnectionString))
            {
                String sqldel = "delete from " + tabla;

                using (SqlCommand commandel = new SqlCommand(sqldel, cndel))
                {
                    cndel.Open();
                    commandel.ExecuteNonQuery();
                    cndel.Close();
                }
            }

            //INSERT TABLA
            using (SqlConnection cn = new SqlConnection(ConfigurationManager.ConnectionStrings["cn"].ConnectionString))

                for (int i = 0; i < tblInvHdr.Count; i++)
                {

                    String sql = "Insert into " + tabla + " (BUKRS,DSUBEXE,DSUBEXO,DSUB5,DSUB10,DTOTOPE,DTOTDESC,DTOTDESCGLOTEM,DTOTANTITEM,DTOTANT,DPORCDESCTOTAL,DDESCTOTAL,DANTICIPO,DREDON,DCOMI,DTOTGRALOPE,DIVA5,DIVA10,DLIQTOTIVA5,DLIQTOTIVA10,DIVACOMI,DTOTIVA,DBASEGRAV5,DBASEGRAV10,DTBASGRAIVA,DTOTALGS,FK_FAC) VALUES (@BUKRS,@DSUBEXE,@DSUBEXO,@DSUB5,@DSUB10,@DTOTOPE,@DTOTDESC,@DTOTDESCGLOTEM,@DTOTANTITEM,@DTOTANT,@DPORCDESCTOTAL,@DDESCTOTAL,@DANTICIPO,@DREDON,@DCOMI,@DTOTGRALOPE,@DIVA5,@DIVA10,@DLIQTOTIVA5,@DLIQTOTIVA10,@DIVACOMI,@DTOTIVA,@DBASEGRAV5,@DBASEGRAV10,@DTBASGRAIVA,@DTOTALGS,@FK_FAC)";

                    using (SqlCommand command = new SqlCommand(sql, cn))
                    {
                        command.Parameters.AddWithValue("@BUKRS", tblInvHdr[i].GetValue("BUKRS"));
                        command.Parameters.AddWithValue("@DSUBEXE", tblInvHdr[i].GetValue("DSUBEXE"));
                        command.Parameters.AddWithValue("@DSUBEXO", tblInvHdr[i].GetValue("DSUBEXO"));
                        command.Parameters.AddWithValue("@DSUB5", tblInvHdr[i].GetValue("DSUB5"));
                        command.Parameters.AddWithValue("@DSUB10", tblInvHdr[i].GetValue("DSUB10"));
                        command.Parameters.AddWithValue("@DTOTOPE", tblInvHdr[i].GetValue("DTOTOPE"));
                        command.Parameters.AddWithValue("@DTOTDESC", tblInvHdr[i].GetValue("DTOTDESC"));
                        command.Parameters.AddWithValue("@DTOTDESCGLOTEM", tblInvHdr[i].GetValue("DTOTDESCGLOTEM"));
                        command.Parameters.AddWithValue("@DTOTANTITEM", tblInvHdr[i].GetValue("DTOTANTITEM"));
                        command.Parameters.AddWithValue("@DTOTANT", tblInvHdr[i].GetValue("DTOTANT"));
                        command.Parameters.AddWithValue("@DPORCDESCTOTAL", tblInvHdr[i].GetValue("DPORCDESCTOTAL"));
                        command.Parameters.AddWithValue("@DDESCTOTAL", tblInvHdr[i].GetValue("DDESCTOTAL"));
                        command.Parameters.AddWithValue("@DANTICIPO", tblInvHdr[i].GetValue("DANTICIPO"));
                        command.Parameters.AddWithValue("@DREDON", tblInvHdr[i].GetValue("DREDON"));
                        command.Parameters.AddWithValue("@DCOMI", tblInvHdr[i].GetValue("DCOMI"));
                        command.Parameters.AddWithValue("@DTOTGRALOPE", tblInvHdr[i].GetValue("DTOTGRALOPE"));
                        command.Parameters.AddWithValue("@DIVA5", tblInvHdr[i].GetValue("DIVA5"));
                        command.Parameters.AddWithValue("@DIVA10", tblInvHdr[i].GetValue("DIVA10"));
                        command.Parameters.AddWithValue("@DLIQTOTIVA5", tblInvHdr[i].GetValue("DLIQTOTIVA5"));
                        command.Parameters.AddWithValue("@DLIQTOTIVA10", tblInvHdr[i].GetValue("DLIQTOTIVA10"));
                        command.Parameters.AddWithValue("@DIVACOMI", tblInvHdr[i].GetValue("DIVACOMI"));
                        command.Parameters.AddWithValue("@DTOTIVA", tblInvHdr[i].GetValue("DTOTIVA"));
                        command.Parameters.AddWithValue("@DBASEGRAV5", tblInvHdr[i].GetValue("DBASEGRAV5"));
                        command.Parameters.AddWithValue("@DBASEGRAV10", tblInvHdr[i].GetValue("DBASEGRAV10"));
                        command.Parameters.AddWithValue("@DTBASGRAIVA", tblInvHdr[i].GetValue("DTBASGRAIVA"));
                        command.Parameters.AddWithValue("@DTOTALGS", tblInvHdr[i].GetValue("DTOTALGS"));
                        command.Parameters.AddWithValue("@FK_FAC", tblInvHdr[i].GetValue("FK_FAC"));





                        cn.Open();
                        command.ExecuteNonQuery();
                        cn.Close();

                    }

                    k = i;
                    label2.Text = k + " Registros insertados en " + tabla;

                }



            ///////////////// CONEXION A POSTGRE //////////////////////////////////////////////////////////////////////////////////////////////////////////////
            using (SqlConnection cndel = new SqlConnection(ConfigurationManager.ConnectionStrings["cn"].ConnectionString))
            {


                String sqlsel = "SELECT DSUBEXE,DSUBEXO,DSUB5,DSUB10,DTOTOPE,DTOTDESC,DTOTDESCGLOTEM,DTOTANTITEM,DTOTANT,DPORCDESCTOTAL,DDESCTOTAL,DANTICIPO,DREDON,DCOMI,DTOTGRALOPE,DIVA5,DIVA10,DLIQTOTIVA5,DLIQTOTIVA10,DIVACOMI,DTOTIVA,DBASEGRAV5,DBASEGRAV10,DTBASGRAIVA,DTOTALGS,FK_FAC from " + tabla + " WHERE BUKRS='UBPY'";


                SqlDataAdapter dataAdapter = new SqlDataAdapter(sqlsel, cndel);

                cndel.Open();
                DataTable dt1 = new DataTable();

                dataAdapter.Fill(dt1);

                //dataGridView2.DataSource = dt1; // MUESTRA EL RESULTADO, SI HAY ALGO 

                cndel.Close();



                using (var client = new SshClient("190.104.168.19", 2222, "root", "Admin1234."))
                {
                    client.Connect();

                    if (!client.IsConnected)
                    {
                        // Display error
                        label3.Text = "No Concectado";
                    }
                    else
                    {
                        label3.Text = "Conectado por SSH";
                    }

                    var port = new ForwardedPortLocal("127.0.0.1", "127.0.0.1", 5432);
                    client.AddForwardedPort(port);
                    port.Start();

                    string connString =
                        $"Server={port.BoundHost};Database=atria;Port={port.BoundPort};" +
                         "User Id=sap_atria;Password=XEt7xAqD1F4e;";

                    using (var conn = new NpgsqlConnection(connString))
                    {
                        conn.Open();


                        foreach (DataRow row in dt1.Rows)
                        {
                            string sql = "INSERT INTO t_factura_st (DSUBEXE,DSUBEXO,DSUB5,DSUB10,DTOTOPE,DTOTDESC,DTOTDESCGLOTEM,DTOTANTITEM,DTOTANT,DPORCDESCTOTAL,DDESCTOTAL,DANTICIPO,DREDON,DCOMI,DTOTGRALOPE,DIVA5,DIVA10,DLIQTOTIVA5,DLIQTOTIVA10,DIVACOMI,DTOTIVA,DBASEGRAV5,DBASEGRAV10,DTBASGRAIVA,DTOTALGS,FK_FAC) VALUES (@DSUBEXE,@DSUBEXO,@DSUB5,@DSUB10,@DTOTOPE,@DTOTDESC,@DTOTDESCGLOTEM,@DTOTANTITEM,@DTOTANT,@DPORCDESCTOTAL,@DDESCTOTAL,@DANTICIPO,@DREDON,@DCOMI,@DTOTGRALOPE,@DIVA5,@DIVA10,@DLIQTOTIVA5,@DLIQTOTIVA10,@DIVACOMI,@DTOTIVA,@DBASEGRAV5,@DBASEGRAV10,@DTBASGRAIVA,@DTOTALGS,@FK_FAC)";

                            NpgsqlDataAdapter da = new NpgsqlDataAdapter(sql, conn);
                            using (NpgsqlCommand command = new NpgsqlCommand(sql, conn))
                            {
                                command.Parameters.AddWithValue("@DSUBEXE", row.Field<string>("DSUBEXE"));
                                command.Parameters.AddWithValue("@DSUBEXO", row.Field<string>("DSUBEXO"));
                                command.Parameters.AddWithValue("@DSUB5", row.Field<string>("DSUB5"));
                                command.Parameters.AddWithValue("@DSUB10", row.Field<string>("DSUB10"));
                                command.Parameters.AddWithValue("@DTOTOPE", row.Field<string>("DTOTOPE"));
                                command.Parameters.AddWithValue("@DTOTDESC", row.Field<string>("DTOTDESC"));
                                command.Parameters.AddWithValue("@DTOTDESCGLOTEM", row.Field<string>("DTOTDESCGLOTEM"));
                                command.Parameters.AddWithValue("@DTOTANTITEM", row.Field<string>("DTOTANTITEM"));
                                command.Parameters.AddWithValue("@DTOTANT", row.Field<string>("DTOTANT"));
                                command.Parameters.AddWithValue("@DPORCDESCTOTAL", row.Field<string>("DPORCDESCTOTAL"));
                                command.Parameters.AddWithValue("@DDESCTOTAL", row.Field<string>("DDESCTOTAL"));
                                command.Parameters.AddWithValue("@DANTICIPO", row.Field<string>("DANTICIPO"));
                                command.Parameters.AddWithValue("@DREDON", row.Field<string>("DREDON"));
                                command.Parameters.AddWithValue("@DCOMI", row.Field<string>("DCOMI"));
                                command.Parameters.AddWithValue("@DTOTGRALOPE", row.Field<string>("DTOTGRALOPE"));
                                command.Parameters.AddWithValue("@DIVA5", row.Field<string>("DIVA5"));
                                command.Parameters.AddWithValue("@DIVA10", row.Field<string>("DIVA10"));
                                command.Parameters.AddWithValue("@DLIQTOTIVA5", row.Field<string>("DLIQTOTIVA5"));
                                command.Parameters.AddWithValue("@DLIQTOTIVA10", row.Field<string>("DLIQTOTIVA10"));
                                command.Parameters.AddWithValue("@DIVACOMI", row.Field<string>("DIVACOMI"));
                                command.Parameters.AddWithValue("@DTOTIVA", row.Field<string>("DTOTIVA"));
                                command.Parameters.AddWithValue("@DBASEGRAV5", row.Field<string>("DBASEGRAV5"));
                                command.Parameters.AddWithValue("@DBASEGRAV10", row.Field<string>("DBASEGRAV10"));
                                command.Parameters.AddWithValue("@DTBASGRAIVA", row.Field<string>("DTBASGRAIVA"));
                                command.Parameters.AddWithValue("@DTOTALGS", row.Field<string>("DTOTALGS"));
                                command.Parameters.AddWithValue("@FK_FAC", row.Field<string>("FK_FAC"));

                            }
                        }

                        conn.Close();
                    }

                    label3.Text = "insertado!";
                    port.Stop();
                    client.Disconnect();
                }



            }

            //////////////////////////////////////////////////////////////////////////////////////////////////
            //////////////////////////////////////////////////////////////////////////////////////////////////


            RfcSessionManager.EndContext(SapRfcDestination);
            MOSTRAR_TABLA(tabla);
            INSERTLOG(tabla);
        }

        private void ZFEI006()
        {
            tabla = "ZFEI006";
            label2.Text = "";
            k = 0;

            RfcDestination SapRfcDestination = connectSAP();
            RfcSessionManager.BeginContext(SapRfcDestination);
            IRfcFunction funcInvHdrs = SapRfcDestination.Repository.CreateFunction("ZFX0001");
            funcInvHdrs.Invoke(SapRfcDestination);
            string cntonHdrs = funcInvHdrs.GetValue(tabla).ToString();
            IRfcTable tblInvHdr = funcInvHdrs.GetTable(tabla);
            tblInvHdr.Append();

            //DELETE TABLA
            using (SqlConnection cndel = new SqlConnection(ConfigurationManager.ConnectionStrings["cn"].ConnectionString))
            {
                String sqldel = "delete from " + tabla;

                using (SqlCommand commandel = new SqlCommand(sqldel, cndel))
                {
                    cndel.Open();
                    commandel.ExecuteNonQuery();
                    cndel.Close();
                }
            }

            //INSERT TABLA
            using (SqlConnection cn = new SqlConnection(ConfigurationManager.ConnectionStrings["cn"].ConnectionString))

                for (int i = 0; i < tblInvHdr.Count; i++)
                {

                    String sql = "Insert into " + tabla + " (BUKRS,ITIPAGO,DDESTIPAG,DMONTIPAG,CMONETIPAG,DDMONETIPAG,DTICAMTIPAG,IDENTARJ,DDESDENTARJ,DRSPROTAR,DRUCPROTAR,DDVPROTAR,IFORPROPA,DCODAUOPE,DNOMTIT,DNUMTARJ,DNUMCHEQ,DBCOEMI,FK_FAC) VALUES (@BUKRS,@ITIPAGO,@DDESTIPAG,@DMONTIPAG,@CMONETIPAG,@DDMONETIPAG,@DTICAMTIPAG,@IDENTARJ,@DDESDENTARJ,@DRSPROTAR,@DRUCPROTAR,@DDVPROTAR,@IFORPROPA,@DCODAUOPE,@DNOMTIT,@DNUMTARJ,@DNUMCHEQ,@DBCOEMI,@FK_FAC)";

                    using (SqlCommand command = new SqlCommand(sql, cn))
                    {


                        command.Parameters.AddWithValue("@BUKRS", tblInvHdr[i].GetValue("BUKRS"));
                        command.Parameters.AddWithValue("@ITIPAGO", tblInvHdr[i].GetValue("ITIPAGO"));
                        command.Parameters.AddWithValue("@DDESTIPAG", tblInvHdr[i].GetValue("DDESTIPAG"));
                        command.Parameters.AddWithValue("@DMONTIPAG", tblInvHdr[i].GetValue("DMONTIPAG"));
                        command.Parameters.AddWithValue("@CMONETIPAG", tblInvHdr[i].GetValue("CMONETIPAG"));
                        command.Parameters.AddWithValue("@DDMONETIPAG", tblInvHdr[i].GetValue("DDMONETIPAG"));
                        command.Parameters.AddWithValue("@DTICAMTIPAG", tblInvHdr[i].GetValue("DTICAMTIPAG"));
                        command.Parameters.AddWithValue("@IDENTARJ", tblInvHdr[i].GetValue("IDENTARJ"));
                        command.Parameters.AddWithValue("@DDESDENTARJ", tblInvHdr[i].GetValue("DDESDENTARJ"));
                        command.Parameters.AddWithValue("@DRSPROTAR", tblInvHdr[i].GetValue("DRSPROTAR"));
                        command.Parameters.AddWithValue("@DRUCPROTAR", tblInvHdr[i].GetValue("DRUCPROTAR"));
                        command.Parameters.AddWithValue("@DDVPROTAR", tblInvHdr[i].GetValue("DDVPROTAR"));
                        command.Parameters.AddWithValue("@IFORPROPA", tblInvHdr[i].GetValue("IFORPROPA"));
                        command.Parameters.AddWithValue("@DCODAUOPE", tblInvHdr[i].GetValue("DCODAUOPE"));
                        command.Parameters.AddWithValue("@DNOMTIT", tblInvHdr[i].GetValue("DNOMTIT"));
                        command.Parameters.AddWithValue("@DNUMTARJ", tblInvHdr[i].GetValue("DNUMTARJ"));
                        command.Parameters.AddWithValue("@DNUMCHEQ", tblInvHdr[i].GetValue("DNUMCHEQ"));
                        command.Parameters.AddWithValue("@DBCOEMI", tblInvHdr[i].GetValue("DBCOEMI"));
                        command.Parameters.AddWithValue("@FK_FAC", tblInvHdr[i].GetValue("FK_FAC"));






                        cn.Open();
                        command.ExecuteNonQuery();
                        cn.Close();

                    }

                    k = i;
                    label2.Text = k + " Registros insertados en " + tabla;

                }



            ///////////////// CONEXION A POSTGRE //////////////////////////////////////////////////////////////////////////////////////////////////////////////
            using (SqlConnection cndel = new SqlConnection(ConfigurationManager.ConnectionStrings["cn"].ConnectionString))
            {


                String sqlsel = "SELECT ITIPAGO,DDESTIPAG,DMONTIPAG,CMONETIPAG,DDMONETIPAG,DTICAMTIPAG,IDENTARJ,DDESDENTARJ,DRSPROTAR,DRUCPROTAR,DDVPROTAR,IFORPROPA,DCODAUOPE,DNOMTIT,DNUMTARJ,DNUMCHEQ,DBCOEMI,FK_FAC from " + tabla + " WHERE BUKRS='UBPY'";


                SqlDataAdapter dataAdapter = new SqlDataAdapter(sqlsel, cndel);

                cndel.Open();
                DataTable dt1 = new DataTable();

                dataAdapter.Fill(dt1);

                //dataGridView2.DataSource = dt1; // MUESTRA EL RESULTADO, SI HAY ALGO 

                cndel.Close();



                using (var client = new SshClient("190.104.168.19", 2222, "root", "Admin1234."))
                {
                    client.Connect();

                    if (!client.IsConnected)
                    {
                        // Display error
                        label3.Text = "No Concectado";
                    }
                    else
                    {
                        label3.Text = "Conectado por SSH";
                    }

                    var port = new ForwardedPortLocal("127.0.0.1", "127.0.0.1", 5432);
                    client.AddForwardedPort(port);
                    port.Start();

                    string connString =
                        $"Server={port.BoundHost};Database=atria;Port={port.BoundPort};" +
                         "User Id=sap_atria;Password=XEt7xAqD1F4e;";

                    using (var conn = new NpgsqlConnection(connString))
                    {



                        foreach (DataRow row in dt1.Rows)
                        {
                            //string sql = "INSERT INTO t_factura_fp (ITIPAGO,DDESTIPAG,DMONTIPAG,CMONETIPAG,DDMONETIPAG,DTICAMTIPAG,IDENTARJ,DDESDENTARJ,DRSPROTAR,DRUCPROTAR,DDVPROTAR,IFORPROPA,DCODAUOPE,DNOMTIT,DNUMTARJ,DNUMCHEQ,DBCOEMI,FK_FAC) VALUES (@ITIPAGO,@DDESTIPAG,@DMONTIPAG,@CMONETIPAG,@DDMONETIPAG,@DTICAMTIPAG,@IDENTARJ,@DDESDENTARJ,@DRSPROTAR,@DRUCPROTAR,@DDVPROTAR,@IFORPROPA,@DCODAUOPE,@DNOMTIT,@DNUMTARJ,@DNUMCHEQ,@DBCOEMI,@FK_FAC)";

                            string sql = "INSERT INTO t_factura_fp (ITIPAGO) VALUES (@ITIPAGO)";

                            NpgsqlDataAdapter da = new NpgsqlDataAdapter(sql, conn);
                            using (NpgsqlCommand command = new NpgsqlCommand(sql, conn))
                            {

                                int ITIPAGO = Convert.ToInt32(row.Field<string>("ITIPAGO"));


                                command.Parameters.AddWithValue("@ITIPAGO", ITIPAGO);
                                /*command.Parameters.AddWithValue("@DDESTIPAG", row.Field<string>("DDESTIPAG"));
                                command.Parameters.AddWithValue("@DMONTIPAG", row.Field<string>("DMONTIPAG"));
                                command.Parameters.AddWithValue("@CMONETIPAG", row.Field<string>("CMONETIPAG"));
                                command.Parameters.AddWithValue("@DDMONETIPAG", row.Field<string>("DDMONETIPAG"));
                                command.Parameters.AddWithValue("@DTICAMTIPAG", row.Field<string>("DTICAMTIPAG"));
                                command.Parameters.AddWithValue("@IDENTARJ", row.Field<string>("IDENTARJ"));
                                command.Parameters.AddWithValue("@DDESDENTARJ", row.Field<string>("DDESDENTARJ"));
                                command.Parameters.AddWithValue("@DRSPROTAR", row.Field<string>("DRSPROTAR"));
                                command.Parameters.AddWithValue("@DRUCPROTAR", row.Field<string>("DRUCPROTAR"));
                                command.Parameters.AddWithValue("@DDVPROTAR", row.Field<string>("DDVPROTAR"));
                                command.Parameters.AddWithValue("@IFORPROPA", row.Field<string>("IFORPROPA"));
                                command.Parameters.AddWithValue("@DCODAUOPE", row.Field<string>("DCODAUOPE"));
                                command.Parameters.AddWithValue("@DNOMTIT", row.Field<string>("DNOMTIT"));
                                command.Parameters.AddWithValue("@DNUMTARJ", row.Field<string>("DNUMTARJ"));
                                command.Parameters.AddWithValue("@DNUMCHEQ", row.Field<string>("DNUMCHEQ"));
                                command.Parameters.AddWithValue("@DBCOEMI", row.Field<string>("DBCOEMI"));
                                command.Parameters.AddWithValue("@FK_FAC", row.Field<string>("FK_FAC"));
                                */


                                conn.Open();
                                command.ExecuteNonQuery();
                                conn.Close();
                            }

                           
                        }

                    }

                    label3.Text = "insertado!";
                    port.Stop();
                    client.Disconnect();
                }



            }

            //////////////////////////////////////////////////////////////////////////////////////////////////
            //////////////////////////////////////////////////////////////////////////////////////////////////

            
            RfcSessionManager.EndContext(SapRfcDestination);
            MOSTRAR_TABLA(tabla);
            INSERTLOG(tabla);
        }

        private void ZFEI010()
        {
            tabla = "ZFEI010";
            label2.Text = "";
            k = 0;

            RfcDestination SapRfcDestination = connectSAP();
            RfcSessionManager.BeginContext(SapRfcDestination);
            IRfcFunction funcInvHdrs = SapRfcDestination.Repository.CreateFunction("ZFX0001");
            funcInvHdrs.Invoke(SapRfcDestination);
            string cntonHdrs = funcInvHdrs.GetValue(tabla).ToString();
            IRfcTable tblInvHdr = funcInvHdrs.GetTable(tabla);
            tblInvHdr.Append();

            //DELETE TABLA
            using (SqlConnection cndel = new SqlConnection(ConfigurationManager.ConnectionStrings["cn"].ConnectionString))
            {
                String sqldel = "delete from " + tabla;

                using (SqlCommand commandel = new SqlCommand(sqldel, cndel))
                {
                    cndel.Open();
                    commandel.ExecuteNonQuery();
                    cndel.Close();
                }
            }

            //INSERT TABLA
            using (SqlConnection cn = new SqlConnection(ConfigurationManager.ConnectionStrings["cn"].ConnectionString))

                for (int i = 0; i < tblInvHdr.Count; i++)
                {

                    String sql = "Insert into " + tabla + " (VBELN,BUKRS,ITIDE,DDESTIDE,DEST,DPUNEXP,DSERIENUM,DNUMDOC,DINFOFISC,DINFOEMI,DFEEMIDE,ITIMP,DDESTIMP,CMONEOPE,DDESMONEOPE,DCONDTICAM,DTICAM,INATREC,ITIOPE,CPAISREC,DDESPAISRE,ITICONTREC,DRUCREC,DDVREC,ITIPIDREC,DTIPIDREC,DNUMIDREC,DNOMREC,DNOMFANREC,DDIRREC,DNUMCASREC,CDEPREC,DDESDEPREC,CDISREC,DDESDISREC,CCIUREC,DDESCIUREC,DTELREC,DCELREC,DEMAILREC,dEmailAdic,dEmailAdmin,DEMAILCC,DEMAILBCC,DCODCLIENTE,IMOTEMI,DDESMOTEMI,DINFADIC,pk_nc) VALUES (@VBELN,@BUKRS,@ITIDE,@DDESTIDE,@DEST,@DPUNEXP,@DSERIENUM,@DNUMDOC,@DINFOFISC,@DINFOEMI,@DFEEMIDE,@ITIMP,@DDESTIMP,@CMONEOPE,@DDESMONEOPE,@DCONDTICAM,@DTICAM,@INATREC,@ITIOPE,@CPAISREC,@DDESPAISRE,@ITICONTREC,@DRUCREC,@DDVREC,@ITIPIDREC,@DTIPIDREC,@DNUMIDREC,@DNOMREC,@DNOMFANREC,@DDIRREC,@DNUMCASREC,@CDEPREC,@DDESDEPREC,@CDISREC,@DDESDISREC,@CCIUREC,@DDESCIUREC,@DTELREC,@DCELREC,@DEMAILREC,@dEmailAdic,@dEmailAdmin,@DEMAILCC,@DEMAILBCC,@DCODCLIENTE,@IMOTEMI,@DDESMOTEMI,@DINFADIC,@pk_nc)";

                    using (SqlCommand command = new SqlCommand(sql, cn))
                    {
                        command.Parameters.AddWithValue("@VBELN", tblInvHdr[i].GetValue("VBELN"));
                        command.Parameters.AddWithValue("@BUKRS", tblInvHdr[i].GetValue("BUKRS"));
                        command.Parameters.AddWithValue("@ITIDE", tblInvHdr[i].GetValue("ITIDE"));
                        command.Parameters.AddWithValue("@DDESTIDE", tblInvHdr[i].GetValue("DDESTIDE"));
                        command.Parameters.AddWithValue("@DEST", tblInvHdr[i].GetValue("DEST"));
                        command.Parameters.AddWithValue("@DPUNEXP", tblInvHdr[i].GetValue("DPUNEXP"));
                        command.Parameters.AddWithValue("@DSERIENUM", tblInvHdr[i].GetValue("DSERIENUM"));
                        command.Parameters.AddWithValue("@DNUMDOC", tblInvHdr[i].GetValue("DNUMDOC"));
                        command.Parameters.AddWithValue("@DINFOFISC", tblInvHdr[i].GetValue("DINFOFISC"));
                        command.Parameters.AddWithValue("@DINFOEMI", tblInvHdr[i].GetValue("DINFOEMI"));
                        command.Parameters.AddWithValue("@DFEEMIDE", tblInvHdr[i].GetValue("DFEEMIDE"));
                        command.Parameters.AddWithValue("@ITIMP", tblInvHdr[i].GetValue("ITIMP"));
                        command.Parameters.AddWithValue("@DDESTIMP", tblInvHdr[i].GetValue("DDESTIMP"));
                        command.Parameters.AddWithValue("@CMONEOPE", tblInvHdr[i].GetValue("CMONEOPE"));
                        command.Parameters.AddWithValue("@DDESMONEOPE", tblInvHdr[i].GetValue("DDESMONEOPE"));
                        command.Parameters.AddWithValue("@DCONDTICAM", tblInvHdr[i].GetValue("DCONDTICAM"));
                        command.Parameters.AddWithValue("@DTICAM", tblInvHdr[i].GetValue("DTICAM"));
                        command.Parameters.AddWithValue("@INATREC", tblInvHdr[i].GetValue("INATREC"));
                        command.Parameters.AddWithValue("@ITIOPE", tblInvHdr[i].GetValue("ITIOPE"));
                        command.Parameters.AddWithValue("@CPAISREC", tblInvHdr[i].GetValue("CPAISREC"));
                        command.Parameters.AddWithValue("@DDESPAISRE", tblInvHdr[i].GetValue("DDESPAISRE"));
                        command.Parameters.AddWithValue("@ITICONTREC", tblInvHdr[i].GetValue("ITICONTREC"));
                        command.Parameters.AddWithValue("@DRUCREC", tblInvHdr[i].GetValue("DRUCREC"));
                        command.Parameters.AddWithValue("@DDVREC", tblInvHdr[i].GetValue("DDVREC"));
                        command.Parameters.AddWithValue("@ITIPIDREC", tblInvHdr[i].GetValue("ITIPIDREC"));
                        command.Parameters.AddWithValue("@DTIPIDREC", tblInvHdr[i].GetValue("DTIPIDREC"));
                        command.Parameters.AddWithValue("@DNUMIDREC", tblInvHdr[i].GetValue("DNUMIDREC"));
                        command.Parameters.AddWithValue("@DNOMREC", tblInvHdr[i].GetValue("DNOMREC"));
                        command.Parameters.AddWithValue("@DNOMFANREC", tblInvHdr[i].GetValue("DNOMFANREC"));
                        command.Parameters.AddWithValue("@DDIRREC", tblInvHdr[i].GetValue("DDIRREC"));
                        command.Parameters.AddWithValue("@DNUMCASREC", tblInvHdr[i].GetValue("DNUMCASREC"));
                        command.Parameters.AddWithValue("@CDEPREC", tblInvHdr[i].GetValue("CDEPREC"));
                        command.Parameters.AddWithValue("@DDESDEPREC", tblInvHdr[i].GetValue("DDESDEPREC"));
                        command.Parameters.AddWithValue("@CDISREC", tblInvHdr[i].GetValue("CDISREC"));
                        command.Parameters.AddWithValue("@DDESDISREC", tblInvHdr[i].GetValue("DDESDISREC"));
                        command.Parameters.AddWithValue("@CCIUREC", tblInvHdr[i].GetValue("CCIUREC"));
                        command.Parameters.AddWithValue("@DDESCIUREC", tblInvHdr[i].GetValue("DDESCIUREC"));
                        command.Parameters.AddWithValue("@DTELREC", tblInvHdr[i].GetValue("DTELREC"));
                        command.Parameters.AddWithValue("@DCELREC", tblInvHdr[i].GetValue("DCELREC"));
                        command.Parameters.AddWithValue("@DEMAILREC", tblInvHdr[i].GetValue("DEMAILREC"));
                        command.Parameters.AddWithValue("@dEmailAdic", tblInvHdr[i].GetValue("dEmailAdic"));
                        command.Parameters.AddWithValue("@dEmailAdmin", tblInvHdr[i].GetValue("dEmailAdmin"));
                        command.Parameters.AddWithValue("@DEMAILCC", tblInvHdr[i].GetValue("DEMAILCC"));
                        command.Parameters.AddWithValue("@DEMAILBCC", tblInvHdr[i].GetValue("DEMAILBCC"));
                        command.Parameters.AddWithValue("@DCODCLIENTE", tblInvHdr[i].GetValue("DCODCLIENTE"));
                        command.Parameters.AddWithValue("@IMOTEMI", tblInvHdr[i].GetValue("IMOTEMI"));
                        command.Parameters.AddWithValue("@DDESMOTEMI", tblInvHdr[i].GetValue("DDESMOTEMI"));
                        command.Parameters.AddWithValue("@DINFADIC", tblInvHdr[i].GetValue("DINFADIC"));
                        command.Parameters.AddWithValue("@pk_nc", tblInvHdr[i].GetValue("pk_nc"));


                        cn.Open();
                        command.ExecuteNonQuery();
                        cn.Close();

                    }

                    k = i;
                    label2.Text = k + " Registros insertados en " + tabla;

                }



            ///////////////// CONEXION A POSTGRE //////////////////////////////////////////////////////////////////////////////////////////////////////////////
            using (SqlConnection cndel = new SqlConnection(ConfigurationManager.ConnectionStrings["cn"].ConnectionString))
            {


                String sqlsel = "SELECT ITIDE,DDESTIDE,DEST,DPUNEXP,DSERIENUM,DNUMDOC,DINFOFISC,DINFOEMI,DFEEMIDE,ITIMP,DDESTIMP,CMONEOPE,DDESMONEOPE,DCONDTICAM,DTICAM,INATREC,ITIOPE,CPAISREC,DDESPAISRE,ITICONTREC,DRUCREC,DDVREC,ITIPIDREC,DTIPIDREC,DNUMIDREC,DNOMREC,DNOMFANREC,DDIRREC,DNUMCASREC,CDEPREC,DDESDEPREC,CDISREC,DDESDISREC,CCIUREC,DDESCIUREC,DTELREC,DCELREC,DEMAILREC,dEmailAdic,dEmailAdmin,DEMAILCC,DEMAILBCC,DCODCLIENTE,IMOTEMI,DDESMOTEMI,DINFADIC,pk_nc from " + tabla + " WHERE BUKRS='UBPY'";


                SqlDataAdapter dataAdapter = new SqlDataAdapter(sqlsel, cndel);

                cndel.Open();
                DataTable dt1 = new DataTable();

                dataAdapter.Fill(dt1);

                //dataGridView2.DataSource = dt1; // MUESTRA EL RESULTADO, SI HAY ALGO 

                cndel.Close();



                using (var client = new SshClient("190.104.168.19", 2222, "root", "Admin1234."))
                {
                    client.Connect();

                    if (!client.IsConnected)
                    {
                        // Display error
                        label3.Text = "No Concectado";
                    }
                    else
                    {
                        label3.Text = "Conectado por SSH";
                    }

                    var port = new ForwardedPortLocal("127.0.0.1", "127.0.0.1", 5432);
                    client.AddForwardedPort(port);
                    port.Start();

                    string connString =
                        $"Server={port.BoundHost};Database=atria;Port={port.BoundPort};" +
                         "User Id=sap_atria;Password=XEt7xAqD1F4e;";

                    using (var conn = new NpgsqlConnection(connString))
                    {
                        conn.Open();


                        foreach (DataRow row in dt1.Rows)
                        {
                            string sql = "INSERT INTO t_credito_cab (ITIDE,DDESTIDE,DEST,DPUNEXP,DSERIENUM,DNUMDOC,DINFOFISC,DINFOEMI,DFEEMIDE,ITIMP,DDESTIMP,CMONEOPE,DDESMONEOPE,DCONDTICAM,DTICAM,INATREC,ITIOPE,CPAISREC,DDESPAISRE,ITICONTREC,DRUCREC,DDVREC,ITIPIDREC,DTIPIDREC,DNUMIDREC,DNOMREC,DNOMFANREC,DDIRREC,DNUMCASREC,CDEPREC,DDESDEPREC,CDISREC,DDESDISREC,CCIUREC,DDESCIUREC,DTELREC,DCELREC,DEMAILREC,dEmailAdic,dEmailAdmin,DEMAILCC,DEMAILBCC,DCODCLIENTE,IMOTEMI,DDESMOTEMI,DINFADIC,pk_nc) VALUES (@ITIDE,@DDESTIDE,@DEST,@DPUNEXP,@DSERIENUM,@DNUMDOC,@DINFOFISC,@DINFOEMI,@DFEEMIDE,@ITIMP,@DDESTIMP,@CMONEOPE,@DDESMONEOPE,@DCONDTICAM,@DTICAM,@INATREC,@ITIOPE,@CPAISREC,@DDESPAISRE,@ITICONTREC,@DRUCREC,@DDVREC,@ITIPIDREC,@DTIPIDREC,@DNUMIDREC,@DNOMREC,@DNOMFANREC,@DDIRREC,@DNUMCASREC,@CDEPREC,@DDESDEPREC,@CDISREC,@DDESDISREC,@CCIUREC,@DDESCIUREC,@DTELREC,@DCELREC,@DEMAILREC,@dEmailAdic,@dEmailAdmin,@DEMAILCC,@DEMAILBCC,@DCODCLIENTE,@IMOTEMI,@DDESMOTEMI,@DINFADIC,@pk_nc)";

                            NpgsqlDataAdapter da = new NpgsqlDataAdapter(sql, conn);
                            using (NpgsqlCommand command = new NpgsqlCommand(sql, conn))
                            {
                                command.Parameters.AddWithValue("@ITIDE", row.Field<string>("ITIDE"));
                                command.Parameters.AddWithValue("@DDESTIDE", row.Field<string>("DDESTIDE"));
                                command.Parameters.AddWithValue("@DEST", row.Field<string>("DEST"));
                                command.Parameters.AddWithValue("@DPUNEXP", row.Field<string>("DPUNEXP"));
                                command.Parameters.AddWithValue("@DSERIENUM", row.Field<string>("DSERIENUM"));
                                command.Parameters.AddWithValue("@DNUMDOC", row.Field<string>("DNUMDOC"));
                                command.Parameters.AddWithValue("@DINFOFISC", row.Field<string>("DINFOFISC"));
                                command.Parameters.AddWithValue("@DINFOEMI", row.Field<string>("DINFOEMI"));
                                command.Parameters.AddWithValue("@DFEEMIDE", row.Field<string>("DFEEMIDE"));
                                command.Parameters.AddWithValue("@ITIMP", row.Field<string>("ITIMP"));
                                command.Parameters.AddWithValue("@DDESTIMP", row.Field<string>("DDESTIMP"));
                                command.Parameters.AddWithValue("@CMONEOPE", row.Field<string>("CMONEOPE"));
                                command.Parameters.AddWithValue("@DDESMONEOPE", row.Field<string>("DDESMONEOPE"));
                                command.Parameters.AddWithValue("@DCONDTICAM", row.Field<string>("DCONDTICAM"));
                                command.Parameters.AddWithValue("@DTICAM", row.Field<string>("DTICAM"));
                                command.Parameters.AddWithValue("@INATREC", row.Field<string>("INATREC"));
                                command.Parameters.AddWithValue("@ITIOPE", row.Field<string>("ITIOPE"));
                                command.Parameters.AddWithValue("@CPAISREC", row.Field<string>("CPAISREC"));
                                command.Parameters.AddWithValue("@DDESPAISRE", row.Field<string>("DDESPAISRE"));
                                command.Parameters.AddWithValue("@ITICONTREC", row.Field<string>("ITICONTREC"));
                                command.Parameters.AddWithValue("@DRUCREC", row.Field<string>("DRUCREC"));
                                command.Parameters.AddWithValue("@DDVREC", row.Field<string>("DDVREC"));
                                command.Parameters.AddWithValue("@ITIPIDREC", row.Field<string>("ITIPIDREC"));
                                command.Parameters.AddWithValue("@DTIPIDREC", row.Field<string>("DTIPIDREC"));
                                command.Parameters.AddWithValue("@DNUMIDREC", row.Field<string>("DNUMIDREC"));
                                command.Parameters.AddWithValue("@DNOMREC", row.Field<string>("DNOMREC"));
                                command.Parameters.AddWithValue("@DNOMFANREC", row.Field<string>("DNOMFANREC"));
                                command.Parameters.AddWithValue("@DDIRREC", row.Field<string>("DDIRREC"));
                                command.Parameters.AddWithValue("@DNUMCASREC", row.Field<string>("DNUMCASREC"));
                                command.Parameters.AddWithValue("@CDEPREC", row.Field<string>("CDEPREC"));
                                command.Parameters.AddWithValue("@DDESDEPREC", row.Field<string>("DDESDEPREC"));
                                command.Parameters.AddWithValue("@CDISREC", row.Field<string>("CDISREC"));
                                command.Parameters.AddWithValue("@DDESDISREC", row.Field<string>("DDESDISREC"));
                                command.Parameters.AddWithValue("@CCIUREC", row.Field<string>("CCIUREC"));
                                command.Parameters.AddWithValue("@DDESCIUREC", row.Field<string>("DDESCIUREC"));
                                command.Parameters.AddWithValue("@DTELREC", row.Field<string>("DTELREC"));
                                command.Parameters.AddWithValue("@DCELREC", row.Field<string>("DCELREC"));
                                command.Parameters.AddWithValue("@DEMAILREC", row.Field<string>("DEMAILREC"));
                                command.Parameters.AddWithValue("@dEmailAdic", row.Field<string>("dEmailAdic"));
                                command.Parameters.AddWithValue("@dEmailAdmin", row.Field<string>("dEmailAdmin"));
                                command.Parameters.AddWithValue("@DEMAILCC", row.Field<string>("DEMAILCC"));
                                command.Parameters.AddWithValue("@DEMAILBCC", row.Field<string>("DEMAILBCC"));
                                command.Parameters.AddWithValue("@DCODCLIENTE", row.Field<string>("DCODCLIENTE"));
                                command.Parameters.AddWithValue("@IMOTEMI", row.Field<string>("IMOTEMI"));
                                command.Parameters.AddWithValue("@DDESMOTEMI", row.Field<string>("DDESMOTEMI"));
                                command.Parameters.AddWithValue("@DINFADIC", row.Field<string>("DINFADIC"));
                                command.Parameters.AddWithValue("@pk_nc", row.Field<string>("pk_nc"));
                                

                            }
                        }

                        conn.Close();
                    }

                    label3.Text = "insertado!";
                    port.Stop();
                    client.Disconnect();
                }



            }

            //////////////////////////////////////////////////////////////////////////////////////////////////
            //////////////////////////////////////////////////////////////////////////////////////////////////



            RfcSessionManager.EndContext(SapRfcDestination);
            MOSTRAR_TABLA(tabla);
            INSERTLOG(tabla);
        }

        private void ZFEI011()
        {
            tabla = "ZFEI011";
            label2.Text = "";
            k = 0;

            RfcDestination SapRfcDestination = connectSAP();
            RfcSessionManager.BeginContext(SapRfcDestination);
            IRfcFunction funcInvHdrs = SapRfcDestination.Repository.CreateFunction("ZFX0001");
            funcInvHdrs.Invoke(SapRfcDestination);
            string cntonHdrs = funcInvHdrs.GetValue(tabla).ToString();
            IRfcTable tblInvHdr = funcInvHdrs.GetTable(tabla);
            tblInvHdr.Append();

            //DELETE TABLA
            using (SqlConnection cndel = new SqlConnection(ConfigurationManager.ConnectionStrings["cn"].ConnectionString))
            {
                String sqldel = "delete from " + tabla;

                using (SqlCommand commandel = new SqlCommand(sqldel, cndel))
                {
                    cndel.Open();
                    commandel.ExecuteNonQuery();
                    cndel.Close();
                }
            }

            //INSERT TABLA
            using (SqlConnection cn = new SqlConnection(ConfigurationManager.ConnectionStrings["cn"].ConnectionString))

                for (int i = 0; i < tblInvHdr.Count; i++)
                {

                    String sql = "Insert into " + tabla + " (BUKRS,DCODINT,DPARARANC,DNCM,DDNCPG,DDNCPE,DGTIN,DGTINPQ,DDESPROSER,CUNIMED,DDESUNIMED,DCANTPROSER,CPAISORIG,DDESPAISORIG,DINFITEM,DPUNIPROSER,DTICAMIT,DTOTBRUOPEITEM,DTOTOPEITEM,DTOTOPEGS,IAFECIVA,DDESAFECIVA,DPROPIVA,DTASAIVA,DBASGRAVIVA,DLIQIVAITEM,FK_NC) VALUES (@BUKRS,@DCODINT,@DPARARANC,@DNCM,@DDNCPG,@DDNCPE,@DGTIN,@DGTINPQ,@DDESPROSER,@CUNIMED,@DDESUNIMED,@DCANTPROSER,@CPAISORIG,@DDESPAISORIG,@DINFITEM,@DPUNIPROSER,@DTICAMIT,@DTOTBRUOPEITEM,@DTOTOPEITEM,@DTOTOPEGS,@IAFECIVA,@DDESAFECIVA,@DPROPIVA,@DTASAIVA,@DBASGRAVIVA,@DLIQIVAITEM,@FK_NC)";

                    using (SqlCommand command = new SqlCommand(sql, cn))
                    {
                        command.Parameters.AddWithValue("@BUKRS", tblInvHdr[i].GetValue("BUKRS"));
                        command.Parameters.AddWithValue("@DCODINT", tblInvHdr[i].GetValue("DCODINT"));
                        command.Parameters.AddWithValue("@DPARARANC", tblInvHdr[i].GetValue("DPARARANC"));
                        command.Parameters.AddWithValue("@DNCM", tblInvHdr[i].GetValue("DNCM"));
                        command.Parameters.AddWithValue("@DDNCPG", tblInvHdr[i].GetValue("DDNCPG"));
                        command.Parameters.AddWithValue("@DDNCPE", tblInvHdr[i].GetValue("DDNCPE"));
                        command.Parameters.AddWithValue("@DGTIN", tblInvHdr[i].GetValue("DGTIN"));
                        command.Parameters.AddWithValue("@DGTINPQ", tblInvHdr[i].GetValue("DGTINPQ"));
                        command.Parameters.AddWithValue("@DDESPROSER", tblInvHdr[i].GetValue("DDESPROSER"));
                        command.Parameters.AddWithValue("@CUNIMED", tblInvHdr[i].GetValue("CUNIMED"));
                        command.Parameters.AddWithValue("@DDESUNIMED", tblInvHdr[i].GetValue("DDESUNIMED"));
                        command.Parameters.AddWithValue("@DCANTPROSER", tblInvHdr[i].GetValue("DCANTPROSER"));
                        command.Parameters.AddWithValue("@CPAISORIG", tblInvHdr[i].GetValue("CPAISORIG"));
                        command.Parameters.AddWithValue("@DDESPAISORIG", tblInvHdr[i].GetValue("DDESPAISORIG"));
                        command.Parameters.AddWithValue("@DINFITEM", tblInvHdr[i].GetValue("DINFITEM"));
                        command.Parameters.AddWithValue("@DPUNIPROSER", tblInvHdr[i].GetValue("DPUNIPROSER"));
                        command.Parameters.AddWithValue("@DTICAMIT", tblInvHdr[i].GetValue("DTICAMIT"));
                        command.Parameters.AddWithValue("@DTOTBRUOPEITEM", tblInvHdr[i].GetValue("DTOTBRUOPEITEM"));
                        command.Parameters.AddWithValue("@DTOTOPEITEM", tblInvHdr[i].GetValue("DTOTOPEITEM"));
                        command.Parameters.AddWithValue("@DTOTOPEGS", tblInvHdr[i].GetValue("DTOTOPEGS"));
                        command.Parameters.AddWithValue("@IAFECIVA", tblInvHdr[i].GetValue("IAFECIVA"));
                        command.Parameters.AddWithValue("@DDESAFECIVA", tblInvHdr[i].GetValue("DDESAFECIVA"));
                        command.Parameters.AddWithValue("@DPROPIVA", tblInvHdr[i].GetValue("DPROPIVA"));
                        command.Parameters.AddWithValue("@DTASAIVA", tblInvHdr[i].GetValue("DTASAIVA"));
                        command.Parameters.AddWithValue("@DBASGRAVIVA", tblInvHdr[i].GetValue("DBASGRAVIVA"));
                        command.Parameters.AddWithValue("@DLIQIVAITEM", tblInvHdr[i].GetValue("DLIQIVAITEM"));
                        command.Parameters.AddWithValue("@FK_NC", tblInvHdr[i].GetValue("FK_NC"));


                        cn.Open();
                        command.ExecuteNonQuery();
                        cn.Close();

                    }

                    k = i;
                    label2.Text = k + " Registros insertados en " + tabla;
                }


            ///////////////// CONEXION A POSTGRE //////////////////////////////////////////////////////////////////////////////////////////////////////////////
            using (SqlConnection cndel = new SqlConnection(ConfigurationManager.ConnectionStrings["cn"].ConnectionString))
            {


                String sqlsel = "SELECT DCODINT,DPARARANC,DNCM,DDNCPG,DDNCPE,DGTIN,DGTINPQ,DDESPROSER,CUNIMED,DDESUNIMED,DCANTPROSER,CPAISORIG,DDESPAISORIG,DINFITEM,DPUNIPROSER,DTICAMIT,DTOTBRUOPEITEM,DTOTOPEITEM,DTOTOPEGS,IAFECIVA,DDESAFECIVA,DPROPIVA,DTASAIVA,DBASGRAVIVA,DLIQIVAITEM,FK_NC from " + tabla + " WHERE BUKRS='UBPY'";


                SqlDataAdapter dataAdapter = new SqlDataAdapter(sqlsel, cndel);

                cndel.Open();
                DataTable dt1 = new DataTable();

                dataAdapter.Fill(dt1);

                //dataGridView2.DataSource = dt1; // MUESTRA EL RESULTADO, SI HAY ALGO 

                cndel.Close();



                using (var client = new SshClient("190.104.168.19", 2222, "root", "Admin1234."))
                {
                    client.Connect();

                    if (!client.IsConnected)
                    {
                        // Display error
                        label3.Text = "No Concectado";
                    }
                    else
                    {
                        label3.Text = "Conectado por SSH";
                    }

                    var port = new ForwardedPortLocal("127.0.0.1", "127.0.0.1", 5432);
                    client.AddForwardedPort(port);
                    port.Start();

                    string connString =
                        $"Server={port.BoundHost};Database=atria;Port={port.BoundPort};" +
                         "User Id=sap_atria;Password=XEt7xAqD1F4e;";

                    using (var conn = new NpgsqlConnection(connString))
                    {
                        conn.Open();


                        foreach (DataRow row in dt1.Rows)
                        {
                            string sql = "INSERT INTO t_credito_detalle (DCODINT,DPARARANC,DNCM,DDNCPG,DDNCPE,DGTIN,DGTINPQ,DDESPROSER,CUNIMED,DDESUNIMED,DCANTPROSER,CPAISORIG,DDESPAISORIG,DINFITEM,DPUNIPROSER,DTICAMIT,DTOTBRUOPEITEM,DTOTOPEITEM,DTOTOPEGS,IAFECIVA,DDESAFECIVA,DPROPIVA,DTASAIVA,DBASGRAVIVA,DLIQIVAITEM,FK_NC) VALUES (@DCODINT,@DPARARANC,@DNCM,@DDNCPG,@DDNCPE,@DGTIN,@DGTINPQ,@DDESPROSER,@CUNIMED,@DDESUNIMED,@DCANTPROSER,@CPAISORIG,@DDESPAISORIG,@DINFITEM,@DPUNIPROSER,@DTICAMIT,@DTOTBRUOPEITEM,@DTOTOPEITEM,@DTOTOPEGS,@IAFECIVA,@DDESAFECIVA,@DPROPIVA,@DTASAIVA,@DBASGRAVIVA,@DLIQIVAITEM,@FK_NC)";

                            NpgsqlDataAdapter da = new NpgsqlDataAdapter(sql, conn);
                            using (NpgsqlCommand command = new NpgsqlCommand(sql, conn))
                            {
                                command.Parameters.AddWithValue("@DCODINT", row.Field<string>("DCODINT"));
                                command.Parameters.AddWithValue("@DPARARANC", row.Field<string>("DPARARANC"));
                                command.Parameters.AddWithValue("@DNCM", row.Field<string>("DNCM"));
                                command.Parameters.AddWithValue("@DDNCPG", row.Field<string>("DDNCPG"));
                                command.Parameters.AddWithValue("@DDNCPE", row.Field<string>("DDNCPE"));
                                command.Parameters.AddWithValue("@DGTIN", row.Field<string>("DGTIN"));
                                command.Parameters.AddWithValue("@DGTINPQ", row.Field<string>("DGTINPQ"));
                                command.Parameters.AddWithValue("@DDESPROSER", row.Field<string>("DDESPROSER"));
                                command.Parameters.AddWithValue("@CUNIMED", row.Field<string>("CUNIMED"));
                                command.Parameters.AddWithValue("@DDESUNIMED", row.Field<string>("DDESUNIMED"));
                                command.Parameters.AddWithValue("@DCANTPROSER", row.Field<string>("DCANTPROSER"));
                                command.Parameters.AddWithValue("@CPAISORIG", row.Field<string>("CPAISORIG"));
                                command.Parameters.AddWithValue("@DDESPAISORIG", row.Field<string>("DDESPAISORIG"));
                                command.Parameters.AddWithValue("@DINFITEM", row.Field<string>("DINFITEM"));
                                command.Parameters.AddWithValue("@DPUNIPROSER", row.Field<string>("DPUNIPROSER"));
                                command.Parameters.AddWithValue("@DTICAMIT", row.Field<string>("DTICAMIT"));
                                command.Parameters.AddWithValue("@DTOTBRUOPEITEM", row.Field<string>("DTOTBRUOPEITEM"));
                                command.Parameters.AddWithValue("@DTOTOPEITEM", row.Field<string>("DTOTOPEITEM"));
                                command.Parameters.AddWithValue("@DTOTOPEGS", row.Field<string>("DTOTOPEGS"));
                                command.Parameters.AddWithValue("@IAFECIVA", row.Field<string>("IAFECIVA"));
                                command.Parameters.AddWithValue("@DDESAFECIVA", row.Field<string>("DDESAFECIVA"));
                                command.Parameters.AddWithValue("@DPROPIVA", row.Field<string>("DPROPIVA"));
                                command.Parameters.AddWithValue("@DTASAIVA", row.Field<string>("DTASAIVA"));
                                command.Parameters.AddWithValue("@DBASGRAVIVA", row.Field<string>("DBASGRAVIVA"));
                                command.Parameters.AddWithValue("@DLIQIVAITEM", row.Field<string>("DLIQIVAITEM"));
                                command.Parameters.AddWithValue("@FK_NC", row.Field<string>("FK_NC"));


                            }
                        }

                        conn.Close();
                    }

                    label3.Text = "insertado!";
                    port.Stop();
                    client.Disconnect();
                }



            }

            //////////////////////////////////////////////////////////////////////////////////////////////////
            //////////////////////////////////////////////////////////////////////////////////////////////////


            RfcSessionManager.EndContext(SapRfcDestination);

            MOSTRAR_TABLA(tabla);
            INSERTLOG(tabla);
        }

        private void ZFEI012()
        {
            tabla = "ZFEI012";
            label2.Text = "";
            k = 0;

            RfcDestination SapRfcDestination = connectSAP();
            RfcSessionManager.BeginContext(SapRfcDestination);
            IRfcFunction funcInvHdrs = SapRfcDestination.Repository.CreateFunction("ZFX0001");
            funcInvHdrs.Invoke(SapRfcDestination);
            string cntonHdrs = funcInvHdrs.GetValue(tabla).ToString();
            IRfcTable tblInvHdr = funcInvHdrs.GetTable(tabla);
            tblInvHdr.Append();

            //DELETE TABLA
            using (SqlConnection cndel = new SqlConnection(ConfigurationManager.ConnectionStrings["cn"].ConnectionString))
            {
                String sqldel = "delete from " + tabla;

                using (SqlCommand commandel = new SqlCommand(sqldel, cndel))
                {
                    cndel.Open();
                    commandel.ExecuteNonQuery();
                    cndel.Close();
                }
            }

            //INSERT TABLA
            using (SqlConnection cn = new SqlConnection(ConfigurationManager.ConnectionStrings["cn"].ConnectionString))

                for (int i = 0; i < tblInvHdr.Count; i++)
                {

                    String sql = "Insert into " + tabla + " (BUKRS,DSUBEXE,DSUBEXO,DSUB5,DSUB10,DTOTOPE,DREDON,DTOTGRALOPE,DIVA5,DIVA10,DLIQTOTIVA5,DLIQTOTIVA10,DTOTIVA,DBASEGRAV5,DBASEGRAV10,DTBASGRAIVA,DTOTALGS,FK_NC) VALUES (@BUKRS,@DSUBEXE,@DSUBEXO,@DSUB5,@DSUB10,@DTOTOPE,@DREDON,@DTOTGRALOPE,@DIVA5,@DIVA10,@DLIQTOTIVA5,@DLIQTOTIVA10,@DTOTIVA,@DBASEGRAV5,@DBASEGRAV10,@DTBASGRAIVA,@DTOTALGS,@FK_NC)";

                    using (SqlCommand command = new SqlCommand(sql, cn))
                    {
                        command.Parameters.AddWithValue("@BUKRS", tblInvHdr[i].GetValue("BUKRS"));
                        command.Parameters.AddWithValue("@DSUBEXE", tblInvHdr[i].GetValue("DSUBEXE"));
                        command.Parameters.AddWithValue("@DSUBEXO", tblInvHdr[i].GetValue("DSUBEXO"));
                        command.Parameters.AddWithValue("@DSUB5", tblInvHdr[i].GetValue("DSUB5"));
                        command.Parameters.AddWithValue("@DSUB10", tblInvHdr[i].GetValue("DSUB10"));
                        command.Parameters.AddWithValue("@DTOTOPE", tblInvHdr[i].GetValue("DTOTOPE"));
                        command.Parameters.AddWithValue("@DREDON", tblInvHdr[i].GetValue("DREDON"));
                        command.Parameters.AddWithValue("@DTOTGRALOPE", tblInvHdr[i].GetValue("DTOTGRALOPE"));
                        command.Parameters.AddWithValue("@DIVA5", tblInvHdr[i].GetValue("DIVA5"));
                        command.Parameters.AddWithValue("@DIVA10", tblInvHdr[i].GetValue("DIVA10"));
                        command.Parameters.AddWithValue("@DLIQTOTIVA5", tblInvHdr[i].GetValue("DLIQTOTIVA5"));
                        command.Parameters.AddWithValue("@DLIQTOTIVA10", tblInvHdr[i].GetValue("DLIQTOTIVA10"));
                        command.Parameters.AddWithValue("@DTOTIVA", tblInvHdr[i].GetValue("DTOTIVA"));
                        command.Parameters.AddWithValue("@DBASEGRAV5", tblInvHdr[i].GetValue("DBASEGRAV5"));
                        command.Parameters.AddWithValue("@DBASEGRAV10", tblInvHdr[i].GetValue("DBASEGRAV10"));
                        command.Parameters.AddWithValue("@DTBASGRAIVA", tblInvHdr[i].GetValue("DTBASGRAIVA"));
                        command.Parameters.AddWithValue("@DTOTALGS", tblInvHdr[i].GetValue("DTOTALGS"));
                        command.Parameters.AddWithValue("@FK_NC", tblInvHdr[i].GetValue("FK_NC"));



                        cn.Open();
                        command.ExecuteNonQuery();
                        cn.Close();

                    }
                    k = i;
                    label2.Text = k + " Registros insertados en " + tabla;






                }


            ///////////////// CONEXION A POSTGRE //////////////////////////////////////////////////////////////////////////////////////////////////////////////
            using (SqlConnection cndel = new SqlConnection(ConfigurationManager.ConnectionStrings["cn"].ConnectionString))
            {


                String sqlsel = "SELECT DSUBEXE,DSUBEXO,DSUB5,DSUB10,DTOTOPE,DREDON,DTOTGRALOPE,DIVA5,DIVA10,DLIQTOTIVA5,DLIQTOTIVA10,DTOTIVA,DBASEGRAV5,DBASEGRAV10,DTBASGRAIVA,DTOTALGS,FK_NC from " + tabla+ " WHERE BUKRS='UBPY'";


                SqlDataAdapter dataAdapter = new SqlDataAdapter(sqlsel, cndel);

                cndel.Open();
                DataTable dt1 = new DataTable();

                dataAdapter.Fill(dt1);

                //dataGridView2.DataSource = dt1; // MUESTRA EL RESULTADO, SI HAY ALGO 

                cndel.Close();



                using (var client = new SshClient("190.104.168.19", 2222, "root", "Admin1234."))
                {
                    client.Connect();

                    if (!client.IsConnected)
                    {
                        // Display error
                        label3.Text = "No Concectado";
                    }
                    else
                    {
                        label3.Text = "Conectado por SSH";
                    }

                    var port = new ForwardedPortLocal("127.0.0.1", "127.0.0.1", 5432);
                    client.AddForwardedPort(port);
                    port.Start();

                    string connString =
                        $"Server={port.BoundHost};Database=atria;Port={port.BoundPort};" +
                         "User Id=sap_atria;Password=XEt7xAqD1F4e;";

                    using (var conn = new NpgsqlConnection(connString))
                    {
                        conn.Open();


                        foreach (DataRow row in dt1.Rows)
                        {
                            string sql = "INSERT INTO t_credito_st (BUKRS,DSUBEXE,DSUBEXO,DSUB5,DSUB10,DTOTOPE,DREDON,DTOTGRALOPE,DIVA5,DIVA10,DLIQTOTIVA5,DLIQTOTIVA10,DTOTIVA,DBASEGRAV5,DBASEGRAV10,DTBASGRAIVA,DTOTALGS,FK_NC) VALUES (@BUKRS,@DSUBEXE,@DSUBEXO,@DSUB5,@DSUB10,@DTOTOPE,@DREDON,@DTOTGRALOPE,@DIVA5,@DIVA10,@DLIQTOTIVA5,@DLIQTOTIVA10,@DTOTIVA,@DBASEGRAV5,@DBASEGRAV10,@DTBASGRAIVA,@DTOTALGS,@FK_NC)";

                            NpgsqlDataAdapter da = new NpgsqlDataAdapter(sql, conn);
                            using (NpgsqlCommand command = new NpgsqlCommand(sql, conn))
                            {
                                command.Parameters.AddWithValue("@DSUBEXE", row.Field<string>("DSUBEXE"));
                                command.Parameters.AddWithValue("@DSUBEXO", row.Field<string>("DSUBEXO"));
                                command.Parameters.AddWithValue("@DSUB5", row.Field<string>("DSUB5"));
                                command.Parameters.AddWithValue("@DSUB10", row.Field<string>("DSUB10"));
                                command.Parameters.AddWithValue("@DTOTOPE", row.Field<string>("DTOTOPE"));
                                command.Parameters.AddWithValue("@DREDON", row.Field<string>("DREDON"));
                                command.Parameters.AddWithValue("@DTOTGRALOPE", row.Field<string>("DTOTGRALOPE"));
                                command.Parameters.AddWithValue("@DIVA5", row.Field<string>("DIVA5"));
                                command.Parameters.AddWithValue("@DIVA10", row.Field<string>("DIVA10"));
                                command.Parameters.AddWithValue("@DLIQTOTIVA5", row.Field<string>("DLIQTOTIVA5"));
                                command.Parameters.AddWithValue("@DLIQTOTIVA10", row.Field<string>("DLIQTOTIVA10"));
                                command.Parameters.AddWithValue("@DTOTIVA", row.Field<string>("DTOTIVA"));
                                command.Parameters.AddWithValue("@DBASEGRAV5", row.Field<string>("DBASEGRAV5"));
                                command.Parameters.AddWithValue("@DBASEGRAV10", row.Field<string>("DBASEGRAV10"));
                                command.Parameters.AddWithValue("@DTBASGRAIVA", row.Field<string>("DTBASGRAIVA"));
                                command.Parameters.AddWithValue("@DTOTALGS", row.Field<string>("DTOTALGS"));
                                command.Parameters.AddWithValue("@FK_NC", row.Field<string>("FK_NC"));

                            }
                        }

                        conn.Close();
                    }

                    label3.Text = "insertado!";
                    port.Stop();
                    client.Disconnect();
                }



            }

            //////////////////////////////////////////////////////////////////////////////////////////////////
            //////////////////////////////////////////////////////////////////////////////////////////////////



            RfcSessionManager.EndContext(SapRfcDestination);

            MOSTRAR_TABLA(tabla);
            INSERTLOG(tabla);
        }

        private void ZFEI013()
        {
            tabla = "ZFEI013";
            label2.Text = "";
            k = 0;

            RfcDestination SapRfcDestination = connectSAP();
            RfcSessionManager.BeginContext(SapRfcDestination);
            IRfcFunction funcInvHdrs = SapRfcDestination.Repository.CreateFunction("ZFX0001");
            funcInvHdrs.Invoke(SapRfcDestination);
            string cntonHdrs = funcInvHdrs.GetValue(tabla).ToString();
            IRfcTable tblInvHdr = funcInvHdrs.GetTable(tabla);
            tblInvHdr.Append();

            //DELETE TABLA
            using (SqlConnection cndel = new SqlConnection(ConfigurationManager.ConnectionStrings["cn"].ConnectionString))
            {
                String sqldel = "delete from " + tabla;

                using (SqlCommand commandel = new SqlCommand(sqldel, cndel))
                {
                    cndel.Open();
                    commandel.ExecuteNonQuery();
                    cndel.Close();
                }
            }

            //INSERT TABLA
            using (SqlConnection cn = new SqlConnection(ConfigurationManager.ConnectionStrings["cn"].ConnectionString))

                for (int i = 0; i < tblInvHdr.Count; i++)
                {

                    String sql = "Insert into " + tabla + " (BUKRS, itipdocaso, ddestipdocaso, DCDCDEREF, DNTIMDI, DESTDOCASO, DPEXPDOCASO, DNUMDOCASO, ITIPODOCASO, dDTipoDocAso, DFECEMIDI, FK_NC) VALUES(@BUKRS, @itipdocaso, @ddestipdocaso, @DCDCDEREF, @DNTIMDI, @DESTDOCASO, @DPEXPDOCASO, @DNUMDOCASO, @ITIPODOCASO, @dDTipoDocAso, @DFECEMIDI, @FK_NC)";

                    using (SqlCommand command = new SqlCommand(sql, cn))
                    {
                        command.Parameters.AddWithValue("@BUKRS", tblInvHdr[i].GetValue("BUKRS"));
                     


                        cn.Open();
                        command.ExecuteNonQuery();
                        cn.Close();

                    }

                    k = i;
                    label2.Text = k + " Registros insertados en " + tabla;
                }


            ///////////////// CONEXION A POSTGRE //////////////////////////////////////////////////////////////////////////////////////////////////////////////

            //INSERT TABLA


            /*using (var client = new SshClient("190.104.168.19", 2222, "root", "Admin1234."))
            {
                client.Connect();

                if (!client.IsConnected)
                {
                    // Display error
                    label3.Text = "No Concectado";
                }
                else
                {
                    label3.Text = "Conectado por SSH";
                }

                var port = new ForwardedPortLocal("127.0.0.1", "127.0.0.1", 5432);
                client.AddForwardedPort(port);
                port.Start();

                string connString =
                    $"Server={port.BoundHost};Database=atria;Port={port.BoundPort};" +
                     "User Id=sap_atria;Password=XEt7xAqD1F4e;";

                using (var conn = new NpgsqlConnection(connString))
                {

                                       
                        for (int i = 0; i < tblInvHdr.Count; i++)
                        {
                          



                        
                        string sql = "INSERT INTO t_credito_docas (itipdocaso,ddestipdocaso,DCDCDEREF,DNTIMDI,DESTDOCASO,DPEXPDOCASO,DNUMDOCASO,ITIPODOCASO,dDTipoDocAso,DFECEMIDI,FK_NC) VALUES (@itipdocaso,@ddestipdocaso,@DCDCDEREF,@DNTIMDI,@DESTDOCASO,@DPEXPDOCASO,@DNUMDOCASO,@ITIPODOCASO,@dDTipoDocAso,@DFECEMIDI,@FK_NC)";
                        //string sql = "INSERT INTO t_credito_docas (ddestipdocaso,DCDCDEREF,DESTDOCASO,DPEXPDOCASO,DNUMDOCASO,dDTipoDocAso,DFECEMIDI,FK_NC) VALUES (@ddestipdocaso,@DCDCDEREF,@DESTDOCASO,@DPEXPDOCASO,@DNUMDOCASO,@dDTipoDocAso,@DFECEMIDI,@FK_NC)";
                        //string sql = "INSERT INTO t_credito_docas (ddestipdocaso,DCDCDEREF,DESTDOCASO,DPEXPDOCASO,DNUMDOCASO,ITIPODOCASO,dDTipoDocAso,FK_NC) VALUES (@ddestipdocaso,@DCDCDEREF,@DESTDOCASO,@DPEXPDOCASO,@DNUMDOCASO,@ITIPODOCASO,@dDTipoDocAso,@FK_NC)";


                        NpgsqlDataAdapter da = new NpgsqlDataAdapter(sql, conn);
                            using (NpgsqlCommand command = new NpgsqlCommand(sql, conn))
                            {
                            
                            int ITIPDOCASO = Convert.ToInt32(tblInvHdr[i].GetValue("ITIPDOCASO"));
                            int DNTIMDI = Convert.ToInt32(tblInvHdr[i].GetValue("DNTIMDI"));
                            int ITIPODOCASO = Convert.ToInt32(tblInvHdr[i].GetValue("ITIPODOCASO"));
                            string DFECEMIDI_TEXTO = Convert.ToString(tblInvHdr[i].GetValue("DFECEMIDI"));
                            DateTime DFECEMIDI = Convert.ToDateTime(DFECEMIDI_TEXTO);

                            command.Parameters.AddWithValue("@BUKRS", tblInvHdr[i].GetValue("BUKRS"));
                            command.Parameters.AddWithValue("@ITIPDOCASO", ITIPDOCASO);
                                command.Parameters.AddWithValue("@dDesTipDocAso", tblInvHdr[i].GetValue("dDesTipDocAso"));
                                command.Parameters.AddWithValue("@DCDCDEREF", tblInvHdr[i].GetValue("DCDCDEREF"));
                                command.Parameters.AddWithValue("@DNTIMDI", DNTIMDI);
                                command.Parameters.AddWithValue("@DESTDOCASO", tblInvHdr[i].GetValue("DESTDOCASO"));
                                command.Parameters.AddWithValue("@DPEXPDOCASO", tblInvHdr[i].GetValue("DPEXPDOCASO"));
                                command.Parameters.AddWithValue("@DNUMDOCASO", tblInvHdr[i].GetValue("DNUMDOCASO"));
                                command.Parameters.AddWithValue("@ITIPODOCASO", ITIPODOCASO);
                                command.Parameters.AddWithValue("@dDTipoDocAso", tblInvHdr[i].GetValue("dDTipoDocAso"));
                                command.Parameters.AddWithValue("@DFECEMIDI", DFECEMIDI);
                                command.Parameters.AddWithValue("@FK_NC", tblInvHdr[i].GetValue("FK_NC"));

                                conn.Open();
                                command.ExecuteNonQuery();
                                conn.Close();
                            }



    

                            k = i;
                        }

                }


    
                        label3.Text = "insertado!";
                        port.Stop();
                  


            }*/

            //////////////////////////////////////////////////////////////////////////////////////////////////
            //////////////////////////////////////////////////////////////////////////////////////////////////


            label2.Text = k + " Registros insertados en " + tabla;

            RfcSessionManager.EndContext(SapRfcDestination);

            MOSTRAR_TABLA(tabla);
            INSERTLOG(tabla);

        }
  

      
        public void INSERTLOG(string tabla)
        {
            using (SqlConnection cnupdate = new SqlConnection(ConfigurationManager.ConnectionStrings["cn"].ConnectionString))
            {
                String updateproc = "INSERT INTO SAP_SERVICES_LOG (PROCESO,FECHA_EJECUCION) VALUES (@PROCESO,@FECHA)";
                var fecha = DateTime.Now;

                using (SqlCommand commandel = new SqlCommand(updateproc, cnupdate))
                {
                    commandel.Parameters.AddWithValue("@PROCESO", tabla );
                    commandel.Parameters.AddWithValue("@FECHA", fecha);
                    cnupdate.Open();
                    commandel.ExecuteNonQuery();
                    cnupdate.Close();
                }
            }
        }

        private void Button3_Click(object sender, EventArgs e)
        {
            ZFEI004();
        }

        private void Button2_Click(object sender, EventArgs e)
        {
         ZFEI003();
        }

        private void Button4_Click(object sender, EventArgs e)
        {
            ZFEI005();
        }

        private void Button5_Click(object sender, EventArgs e)
        {
            ZFEI006();
        }

        private void Button6_Click(object sender, EventArgs e)
        {
            ZFEI010();
        }

        private void Button7_Click(object sender, EventArgs e)
        {
            ZFEI011();
        }

        private void Button8_Click(object sender, EventArgs e)
        {
            ZFEI012();
        }

        private void Button9_Click(object sender, EventArgs e)
        {
         ZFEI013();
        }


        private void MOSTRAR_TABLA (string tabla)
        {
            using (SqlConnection conn = new SqlConnection(ConfigurationManager.ConnectionStrings["cn"].ConnectionString))
            {
                conn.Open();

                string sql = "SELECT  * FROM " + tabla;

                SqlDataAdapter da = new SqlDataAdapter(sql, conn);

                ds.Reset();

                da.Fill(ds);

                dt = ds.Tables[0];

                dataGridView1.DataSource = dt;

                conn.Close();



            }
        }

        private void Button10_Click(object sender, EventArgs e)
        {

          

        }

        private void DataGridView1_CellContentClick(object sender, DataGridViewCellEventArgs e)
        {

        }
    }
}
