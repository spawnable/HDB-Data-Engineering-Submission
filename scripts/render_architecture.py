"""Render editable SVG architecture diagrams and submission PNGs using AWS icons.

The embedded service/resource SVGs are unmodified assets from the official AWS
Architecture Icon Pack (Q1 2023). This script only lays out boxes, labels and links.
"""
import base64
from html import escape
from pathlib import Path
import cairosvg

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'architecture'
INK = '#172B4D'
BLUE = '#2563A6'
GREEN = '#1A7D61'
PURPLE = '#7252AA'


class Drawing:
    def __init__(self, width, height):
        self.width, self.height = width, height
        self.items = [f'<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
                      '<defs>']
        for name, color in [('blue', BLUE), ('green', GREEN), ('purple', PURPLE)]:
            self.items.append(f'<marker id="{name}" markerWidth="10" markerHeight="10" refX="8" refY="4" orient="auto-start-reverse"><path d="M 0 0 L 8 4 L 0 8 z" fill="{color}"/></marker>')
        self.items.extend(['</defs>', f'<rect width="{width}" height="{height}" fill="#FAFBFE"/>'])

    def box(self, x, y, w, h, fill='#FFFFFF', stroke='#C4CEDC', radius=10, dash=False):
        self.items.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{radius}" fill="{fill}" stroke="{stroke}" stroke-width="2"'+(' stroke-dasharray="8 5"' if dash else '')+'/>')

    def text(self, x, y, lines, size=18, color=INK, bold=False, line_height=None):
        if isinstance(lines, str): lines = lines.split('\n')
        lh = line_height or size * 1.4
        for i, text in enumerate(lines):
            self.items.append(f'<text x="{x}" y="{y+i*lh}" fill="{color}" font-family="DejaVu Sans,sans-serif" font-size="{size}" font-weight="{700 if bold else 400}">{escape(text)}</text>')

    def icon(self, name, x, y, size=46):
        data = base64.b64encode((OUT / 'icons' / f'{name}.svg').read_bytes()).decode()
        self.items.append(f'<image x="{x}" y="{y}" width="{size}" height="{size}" xlink:href="data:image/svg+xml;base64,{data}"/>')

    def node(self, x, y, w, h, title, body='', icon=None, fill='#FFFFFF'):
        self.box(x, y, w, h, fill=fill)
        offset = 74 if icon else 18
        if icon: self.icon(icon, x+15, y+17)
        self.text(x+offset, y+30, title, size=18, bold=True, line_height=23)
        if body:
            body_lines = body.split('\n')
            first_baseline = y+h-20-(len(body_lines)-1)*22
            body_x = x+offset if icon and first_baseline < y+80 else x+17
            self.text(body_x, first_baseline, body_lines, size=15, line_height=22)

    def frame(self, x, y, w, h, title, color=BLUE, fill='#F2F7FD', size=20):
        self.box(x, y, w, h, fill=fill, stroke=color, radius=12)
        self.text(x+20, y+31, title, size=size, color=color, bold=True)

    def arrow(self, points, color='blue', dashed=False, both=False):
        colors={'blue': BLUE, 'green': GREEN, 'purple': PURPLE}
        path='M '+' L '.join(f'{x} {y}' for x,y in points)
        self.items.append(f'<path d="{path}" fill="none" stroke="{colors[color]}" stroke-width="3" stroke-linejoin="round" marker-end="url(#{color})"'+(f' marker-start="url(#{color})"' if both else '')+(' stroke-dasharray="8 6"' if dashed else '')+'/>')

    def save(self, name):
        svg='\n'.join(self.items+['</svg>'])
        (OUT/f'{name}.svg').write_text(svg,encoding='utf-8')
        cairosvg.svg2png(bytestring=svg.encode(),write_to=str(OUT/f'{name}.png'),scale=2)


def ingestion():
    d=Drawing(1900,1480)
    d.text(50,58,'01  |  Batch ingestion and governed processing',size=34,bold=True)
    d.text(50,98,'Architecture proposal / Singapore Region / source-to-S3 flow / task runs have no public IPs',size=19)
    d.frame(335,140,1525,1090,'AWS account / ap-southeast-1',fill='#FFFFFF',color=BLUE)
    d.frame(365,190,1465,150,'Regional orchestration',fill='#F7F9FC',color='#77879B',size=18)
    d.node(405,238,280,75,'EventBridge\nScheduler',icon='Amazon-EventBridge')
    d.node(840,238,330,75,'Step Functions Standard',icon='AWS-Step-Functions')
    d.arrow([(685,275),(840,275)],dashed=True)
    d.text(1200,256,['Retry / timeout / audit','Input: object references, never file bytes'],size=16)

    d.frame(365,370,1465,575,'VPC A / platform network / matching subnets in AZ A and AZ B',color=GREEN,fill='#F5FAF8')
    for x,w,title,fill in [(390,260,'Public egress', '#FFF7F1'),(675,280,'Inspection', '#FAF5FE'),
                           (980,340,'Private ingestion', '#F2F8FD'),(1345,460,'Isolated processing', '#F2F8FD')]:
        d.frame(x,425,w,345,title,color='#8995A8',fill=fill,size=18)
    d.node(55,540,235,150,'data.gov.sg','Public HTTPS endpoints\nAPI + approved file host',fill='#F6F0FB')
    d.text(58,727,['Conceptual production source.','The submitted pipeline uses','only the five uploaded CSVs.'],size=16)
    d.node(420,480,200,92,'Internet\ngateway',icon='Internet-Gateway')
    d.node(420,655,200,85,'NAT gateway',icon='NAT-Gateway')
    d.node(700,558,230,130,'AWS Network\nFirewall','Allowlisted TLS domains\nAZ-local symmetric routes',icon='AWS-Network-Firewall')
    d.node(1020,558,260,130,'ECS on Fargate\nDownload task','Stream bytes + SHA-256\nBounded retries / multipart',icon='AWS-Fargate')
    d.node(1390,558,370,130,'ECS on Fargate\nETL task','Versioned image / same Python rules\nValidate, deduplicate, score, transform',icon='AWS-Fargate')
    d.arrow([(290,610),(330,610),(330,525),(420,525)],color='purple',both=True)
    d.arrow([(520,572),(520,655)],color='purple',both=True)
    d.arrow([(620,697),(660,697),(660,621),(700,621)],color='purple',both=True)
    d.arrow([(930,621),(1020,621)],color='purple',both=True)
    d.text(60,485,'Public TLS leg only',size=17,color=PURPLE,bold=True)
    d.arrow([(1090,313),(1090,350),(1235,350),(1235,558)],dashed=True)
    d.arrow([(1135,313),(1135,350),(1575,350),(1575,558)],dashed=True)
    d.text(1190,329,'RunTask.sync: ingest, then ETL',size=16,color=BLUE)
    d.text(1000,731,'Inbound denied',size=15)
    d.text(1370,729,'No internet route',size=15,bold=True)
    d.node(750,832,660,86,'VPC A S3 gateway endpoint','Associated with ingestion and processing route tables',icon='Endpoints',fill='#FFFFFF')
    d.arrow([(1150,688),(1150,832)],color='green')
    d.arrow([(1575,688),(1575,802),(1375,802),(1375,832)],color='green',both=True)
    d.text(994,802,'Raw upload',size=16,color=GREEN)
    d.text(1400,756,'ETL reads / writes',size=15,color=GREEN)
    d.node(740,1022,340,128,'Amazon S3 / Raw','Original bytes + versions + manifest\nIncomplete uploads are not published',icon='Amazon-Simple-Storage-Service')
    d.node(1155,1022,620,128,'Amazon S3 / derived data','Cleaned  |  Transformed  |  Quarantined  |  Hashed\nParquet / quality reports / manifest published after checks',icon='Amazon-Simple-Storage-Service')
    d.arrow([(900,918),(900,1022)],color='green',both=True)
    d.arrow([(1280,918),(1280,980),(1450,980),(1450,1022)],color='green',both=True)
    d.text(741,1188,'S3 is a regional AWS service, outside the VPC. Data access uses the VPC endpoint.',size=17)

    d.node(50,1260,550,145,'Security boundaries','SSE-KMS / S3 Block Public Access / least privilege\nDownload role writes Raw only; ETL role writes derived data\nInterface endpoints: ECR API + DKR, Logs, Glue, KMS',icon='AWS-Identity-and-Access-Management')
    d.node(625,1260,565,145,'Operations and recovery','CloudWatch metrics / CloudTrail / firewall and VPC logs\nManifest + checksum + rule version make replay auditable\nAlarm on failures, missed schedules and quality drift',icon='Amazon-CloudWatch')
    d.node(1215,1260,635,145,'Files over 100 MB','Bounded buffers / multipart upload / abort failed uploads\nFargate storage and timeout sized for file + image + headroom\nScale ETL separately when the scoped data exceed memory',icon='AWS-Fargate')
    d.text(50,1448,'Dashed blue: orchestration    Purple: public-source request/response    Green: private S3 data path    Icons: official AWS pack',size=16)
    d.save('01_batch_ingestion')


def exploitation():
    d=Drawing(1900,1440)
    d.text(50,58,'02  |  Private Tableau access to Athena and S3',size=34,bold=True)
    d.text(50,98,'Architecture proposal / separate private VPCs / no NAT or internet route needed for query and result traffic',size=19)
    d.frame(30,150,1840,1120,'AWS account / ap-southeast-1',fill='#FFFFFF')
    d.frame(60,220,1050,800,'VPC B / Tableau network / private subnets in two AZs',color=GREEN,fill='#F5FAF8')
    d.frame(90,285,330,680,'Application subnets',size=18,color='#8995A8',fill='#F8FAFD')
    d.frame(455,285,620,680,'Endpoint subnets / route-table associations',size=18,color='#8995A8',fill='#F8FAFD')
    d.frame(1150,220,670,1015,'Regional AWS services / outside the VPCs',size=18,color='#8995A8',fill='#F6F8FC')
    d.node(120,355,270,145,'Tableau Server\non Amazon EC2','Built-in Athena connector\nCompatible JDBC driver',icon='Amazon-EC2')
    d.node(120,615,270,152,'Dedicated IAM role','Short-lived credentials\nInstance profile (assumed)\nValidate driver/version support',icon='AWS-Identity-and-Access-Management')
    d.text(116,819,['Corporate users connect over','private enterprise connectivity.','Administrative traffic is separate','from query and result traffic.'],size=16)
    d.node(750,345,285,110,'Athena interface\nendpoint','Private DNS / AZ A + B ENIs',icon='AWS-PrivateLink')
    d.node(750,535,285,90,'Glue interface\nendpoint','Private DNS / metadata',icon='AWS-PrivateLink')
    d.node(750,725,285,110,'VPC B S3\ngateway endpoint','S3 prefix-list route / bucket policy',icon='Endpoints')
    d.text(487,891,['Optional client endpoints: regional STS, Logs, KMS (443).','Add only when the authentication/driver path uses them.'],size=15)
    d.node(1390,345,370,110,'Amazon Athena\nDedicated workgroup','Enforced output location + SSE-KMS',icon='Amazon-Athena')
    d.node(1390,535,370,90,'AWS Glue Data Catalog','Versioned schema / tables and views',icon='AWS-Glue')
    d.node(1390,725,370,110,'Amazon S3 / curated','Approved Parquet datasets\nRead-only approved prefixes',icon='Amazon-Simple-Storage-Service')
    d.node(1390,925,370,110,'Amazon S3 / results','Separate prefix or bucket / SSE-KMS\nRestricted access / lifecycle',icon='Amazon-Simple-Storage-Service')
    d.arrow([(390,400),(750,400)],both=True)
    d.text(452,378,'TLS 443 API + 444 streaming',size=17,color=BLUE)
    d.arrow([(1035,400),(1390,400)],both=True)
    d.arrow([(390,455),(575,455),(575,580),(750,580)],both=True)
    d.text(592,562,'TLS 443',size=17,color=BLUE)
    d.arrow([(1035,580),(1390,580)],both=True)
    d.arrow([(390,480),(515,480),(515,780),(750,780)],color='green',both=True)
    d.text(541,759,'HTTPS 443',size=17,color=GREEN)
    d.arrow([(1035,780),(1390,780)],color='green',both=True)
    d.arrow([(1240,780),(1240,980),(1390,980)],color='green',both=True)
    d.text(1118,872,['Direct S3','result fetch'],size=15,color=GREEN)
    d.arrow([(1575,455),(1575,535)],dashed=True)
    d.text(1595,501,'Metadata',size=15,color=BLUE)
    d.arrow([(1760,400),(1798,400),(1798,780),(1760,780)],color='green',dashed=True)
    d.arrow([(1798,780),(1798,980),(1760,980)],color='green',dashed=True)
    d.text(1395,682,'Athena accesses S3 with caller permissions',size=15,color=GREEN)
    d.text(1395,1081,['Service-to-service calls stay on the AWS network.','They do not traverse your VPC endpoint.','Bucket conditions must allow this authorized path.'],size=16)

    d.frame(60,1060,1050,175,'VPC A / isolated ETL (see diagram 01)',color=GREEN,fill='#F5FAF8',size=18)
    d.node(95,1110,305,92,'ECS on Fargate','Publish validated data only',icon='AWS-Fargate')
    d.node(560,1110,380,92,'VPC A S3 gateway endpoint','Separate from the endpoint in VPC B',icon='Endpoints')
    d.arrow([(400,1156),(560,1156)],color='green')
    d.arrow([(940,1156),(1310,1156),(1310,810),(1390,810)],color='green')
    d.text(80,1309,'No VPC peering is required for this design: both VPCs reach the regional services through their own endpoints.',size=19,bold=True)
    d.text(80,1347,'Athena endpoint SG: 443/444 from Tableau. Glue endpoint SG: 443 from Tableau. S3 gateway endpoints use routes and policies, not SGs.',size=17)
    d.text(80,1380,'Query policies: approved workgroup, Glue metadata, S3 prefixes and KMS keys. Workgroup users can read one another\'s results unless further isolated.',size=16)
    d.text(80,1412,'Dashed: AWS service calls    Solid: client paths    Icons: official AWS pack    Authentication and ports must be checked with the installed Tableau driver.',size=15)
    d.save('02_private_analytics')


if __name__ == '__main__':
    ingestion()
    exploitation()
    print('Rendered two SVGs and two 2x PNGs')
