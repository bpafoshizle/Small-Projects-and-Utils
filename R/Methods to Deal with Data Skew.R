# Optimise: http://rstudio-pubs-static.s3.amazonaws.com/1563_1ae2544c0e324b9bb7f6e63cf8f9e098.html

# BoxCox: http://stats.stackexchange.com/questions/1444/how-should-i-transform-non-negative-data-including-zeros
# Practical BoxCox: http://wolfweb.unr.edu/~zal/STAT755/Lab3.R

# https://en.wikipedia.org/wiki/Power_transform#Box.E2.80.93Cox_transformation
# https://en.wikipedia.org/wiki/Truncated_normal_distribution

require(RODBC)
require(e1071)
require(geoR)
require(MASS)
require(car)

ch <- odbcConnect(dsn='TRA_Prod', uid='MILLERBARR', pwd='feD3gak')

rpd <- sqlQuery(ch, "select
            		store_eff_begin_date,
            		count(*) as rec_count
            	from tra_mart_mss.dbo.dim_store ds
            	where store_insert_date between cast(getdate()-366 as date) and cast(getdate()-1 as date)
            	group by store_eff_begin_date")


skew.score <- function(c, x) (skewness(log(x + c)))^2
cval <- seq(0, 20, l = 101)
skew = cval * 0
for(i in  1:length(cval)) skew[i] <- skewness(log(cval[i] + rpd$rec_count))
plot(cval, skew, type="l", ylab = expression(b[3](c)), xlab=expression(c))
abline(h = 0, lty = 3)

best.c <- optimise(skew.score, c(0,20), x=rpd$rec_count)$minimum
rpd$rec_count_transformed <- log(rpd$rec_count + best.c)



#geoR method gives lambda:
boxcox.fit = boxcoxfit(rpd$rec_count)

#MASS method gives nice plot with CI
boxcox.fit.mass = boxcox(lm(rpd$rec_count~1))

# car method gives lambda and transform function, which is nicest
pt = powerTransform(rpd$rec_count)
rpd$rec_count_powerTransformed = bcPower(rpd$rec_count, pt$lambda)

par(mfrow = c(1,1))
hist(rpd$rec_count)
skewness(rpd$rec_count)
skewness(log(rpd$rec_count))


skewness(rpd$rec_count)
skewness(log(rpd$rec_count))
skewness(rpd$rec_count_transformed)
skewness(rpd$rec_count_powerTransformed)

par(mfrow = c(2,2))
hist(rpd$rec_count)
hist(rpd$rec_count_transformed, col = "azure")
hist(rpd$rec_count_powerTransformed, col = "magenta")


mean(rpd$rec_count_transformed)
sd(rpd$rec_count_transformed)

mean(rpd$rec_count_powerTransformed)
sd(rpd$rec_count_powerTransformed)

